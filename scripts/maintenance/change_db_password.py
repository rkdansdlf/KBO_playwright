"""Change a PostgreSQL role password and keep every local credential store in sync.

Why this exists
---------------
The operational database (``bega_prod`` on the ``kbo_postgres`` container) is
reached over Tailscale by the scheduler and API containers. Their credential
reaches them through two *independent* paths:

1. ``.env`` -> ``DATABASE_URL`` (password embedded in the URL)
2. ``.env`` -> ``BEGA_PROD_PASSWORD`` -> ``docker-compose.local.yml`` rebuilds
   ``DATABASE_URL`` from that variable

Updating only one of the two is a silent outage: the compose override rebuilds
``DATABASE_URL`` from the stale variable, so the recreated containers cannot
authenticate at all. This script updates both in one pass and then proves the
result with a real authenticated query.

Password handling
-----------------
The plaintext never reaches stdout, the transcript, or the process argument
list. It is handed to ``psql`` through stdin and to ``docker exec`` through a
transient ``PGPASSWORD`` environment variable. Generated secrets are written to
a local file outside the repository instead of being printed.

Usage
-----
    # Dry run: prints the plan and the affected credential stores.
    python scripts/maintenance/change_db_password.py --role bega_prod_user --database bega_prod

    # Apply: alter the role, sync .env, verify, recreate the app containers.
    python scripts/maintenance/change_db_password.py --role bega_prod_user --database bega_prod --apply

Exit codes
----------
0  success (or dry-run finished cleanly)
1  a step failed
2  invalid arguments or unmet preconditions
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_ENV_FILE = REPO_ROOT / ".env"
DEFAULT_SECRET_DIR = Path("D:/server-data/secrets")

# Characters that survive both a URL path segment and a compose variable
# substitution without escaping. Anything else can silently break the
# `DATABASE_URL` that docker-compose.local.yml assembles.
URL_SAFE_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._~-")
SYMBOL_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
SYMBOL_LENGTH = 40
NEW_CREDENTIAL_ENV_VAR = "KBO_NEW_DB_PASSWORD"


class StepError(RuntimeError):
    """Raised when a procedural step fails and the run must stop."""


def run_command(
    argv: list[str],
    *,
    stdin: str | None = None,
    cwd: Path | None = None,
    env_overrides: dict[str, str] | None = None,
) -> tuple[int, str, str]:
    """Run a command and capture its output.

    Args:
        argv: Argument vector. Never contains a secret.
        stdin: Optional text piped to the process.
        cwd: Optional working directory.
        env_overrides: Optional environment variables layered on top of the current ones.

    Returns:
        Tuple of (returncode, stdout, stderr).

    """
    env = dict(os.environ)
    if env_overrides:
        env.update(env_overrides)
    proc = subprocess.run(
        argv,
        input=stdin,
        text=True,
        capture_output=True,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def container_is_running(container: str) -> bool:
    """Report whether a docker container is currently running."""
    code, out, _ = run_command(["docker", "inspect", "-f", "{{.State.Running}}", container])
    return code == 0 and out.strip() == "true"


def psql(
    container: str,
    *,
    host: str | None = None,
    port: int | None = None,
    user: str,
    database: str,
    password: str | None = None,
    sql: str | None = None,
) -> tuple[int, str, str]:
    """Run a query through ``psql`` inside a container.

    Args:
        container: Container holding the PostgreSQL server.
        host: Optional TCP host. Omitted means the local unix socket.
        port: Optional TCP port.
        user: Role to authenticate as.
        database: Database to connect to.
        password: Optional password, passed via a transient PGPASSWORD.
        sql: Statement to execute. Defaults to a trivial probe.

    Returns:
        Tuple of (returncode, stdout, stderr).

    """
    argv = ["docker", "exec"]
    if password is not None:
        argv += ["-e", f"PGPASSWORD={password}"]
    argv += [container, "psql", "-U", user, "-d", database, "-At", "-v", "ON_ERROR_STOP=1"]
    if host is not None:
        argv += ["-h", host]
    if port is not None:
        argv += ["-p", str(port)]
    argv += ["-c", sql if sql is not None else "SELECT 1"]
    return run_command(argv)


def alter_role_password(container: str, role: str, password: str) -> None:
    """Set a role's password through the local superuser socket.

    The statement is piped through stdin so the secret never appears in the
    docker command line.

    Args:
        container: Container holding the PostgreSQL server.
        role: Role whose password is replaced.
        password: New plaintext password.

    Raises:
        StepError: If the server rejects the statement.

    """
    escaped = password.replace("'", "''")
    statement = f"ALTER ROLE \"{role}\" WITH PASSWORD '{escaped}';\n"
    code, out, err = run_command(
        [
            "docker",
            "exec",
            "-i",
            container,
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
        ],
        stdin=statement,
    )
    if code != 0:
        message = f"ALTER ROLE failed: {(err or out).strip()[:400]}"
        raise StepError(message)


def verify_login(
    container: str,
    *,
    host: str,
    port: int,
    role: str,
    database: str,
    password: str,
) -> bool:
    """Check whether a role can authenticate and run a query over TCP."""
    code, out, _ = psql(
        container,
        host=host,
        port=port,
        user=role,
        database=database,
        password=password,
        sql="SELECT current_user",
    )
    return code == 0 and out.strip() == role


def generate_secret() -> str:
    """Return a fresh URL-safe random secret."""
    return "".join(secrets.choice(SYMBOL_ALPHABET) for _ in range(SYMBOL_LENGTH))


def validate_secret(password: str) -> None:
    """Reject secrets that would corrupt a URL or a compose substitution.

    Args:
        password: Candidate password.

    Raises:
        StepError: If the password contains characters outside the safe set.

    """
    if not password:
        message = "password is empty"
        raise StepError(message)
    unsafe = sorted({char for char in password if char not in URL_SAFE_CHARS})
    if unsafe:
        message = f"password contains characters that are unsafe in a URL/compose variable: {unsafe}"
        raise StepError(message)


def write_secret_file(path: Path, password: str) -> None:
    """Persist a generated password to a local file that only the owner can read.

    Args:
        path: Destination file.
        password: Plaintext password.

    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(password + "\n", encoding="utf-8")
    if os.name == "nt":
        username = os.environ.get("USERNAME", "")
        subprocess.run(
            ["icacls", str(path), "/inheritance:r", "/grant:r", f"{username}:F"],
            capture_output=True,
            text=True,
            check=False,
        )


def resolve_password(args: argparse.Namespace, *, persist: bool = True) -> tuple[str, Path | None]:
    """Resolve the new password from the configured source.

    Priority: explicit file, environment variable, generated value.

    Args:
        args: Parsed command line arguments.
        persist: Whether a generated password is written to the secret file.

    Returns:
        Tuple of (password, secret_file_written_or_None).

    """
    if args.password_file is not None:
        raw = args.password_file.read_text(encoding="utf-8").strip()
        validate_secret(raw)
        return raw, None

    from_env = os.environ.get(NEW_CREDENTIAL_ENV_VAR)
    if from_env:
        validate_secret(from_env)
        return from_env, None

    generated = generate_secret()
    validate_secret(generated)
    if not persist:
        return generated, None
    write_secret_file(args.secret_out, generated)
    return generated, args.secret_out


def env_var_name_for(database: str) -> str:
    """Derive the .env variable that holds the password for a database."""
    return f"{database.upper()}_PASSWORD"


def update_env_file(env_path: Path, password: str, env_var: str) -> list[str]:
    """Rewrite the credential stores inside an env file.

    Two independent stores are updated: the password embedded in
    ``DATABASE_URL`` and the standalone password variable.

    Args:
        env_path: Path to the env file.
        password: New plaintext password.
        env_var: Name of the standalone password variable.

    Returns:
        List of human readable notes describing what changed.

    Raises:
        StepError: If the file cannot be read or the DATABASE_URL is missing.

    """
    if not env_path.exists():
        message = f"env file not found: {env_path}"
        raise StepError(message)

    original = env_path.read_text(encoding="utf-8")
    lines = original.splitlines()
    notes: list[str] = []
    url_pattern = re.compile(r"(?P<prefix>postgresql(?:\+\w+)?://[^:/\s]+:)[^@\s]+(?P<suffix>@[^\s]*)")
    encoded = quote(password, safe="")

    updated: list[str] = []
    url_hits = 0
    var_hits = 0
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("DATABASE_URL=") and not stripped.startswith("#"):
            replaced, count = url_pattern.subn(rf"\g<prefix>{encoded}\g<suffix>", line)
            url_hits += count
            updated.append(replaced)
            continue
        if stripped.startswith(f"{env_var}=") and not stripped.startswith("#"):
            updated.append(f"{env_var}={password}")
            var_hits += 1
            continue
        updated.append(line)

    if url_hits == 0:
        message = f"DATABASE_URL not found in {env_path}; refusing to write a partial change"
        raise StepError(message)
    if var_hits == 0:
        notes.append(f"WARNING: {env_var} not present in {env_path}; only DATABASE_URL was updated")

    backup = env_path.with_name(f"{env_path.name}.bak_{time.strftime('%Y%m%d_%H%M%S')}")
    backup.write_text(original, encoding="utf-8")
    env_path.write_text("\n".join(updated) + "\n", encoding="utf-8")

    notes.append(f"DATABASE_URL password updated ({url_hits} line(s))")
    notes.append(f"{env_var} updated ({var_hits} line(s))")
    notes.append(f"backup written to {backup}")
    return notes


def compose_up(compose_files: list[str], workdir: Path, services: list[str]) -> tuple[int, str, str]:
    """Recreate compose services so they pick up the new credential.

    The build is skipped on purpose: the running images are older than the
    current Dockerfile, and rebuilding would swap the scheduler from the
    Python 3.12 image onto the 3.14 image. That is an upgrade, not a
    credential change.

    Args:
        compose_files: Compose files to merge, in order.
        workdir: Directory the compose project lives in.
        services: Service names to recreate.

    Returns:
        Tuple of (returncode, stdout, stderr).

    """
    argv = ["docker", "compose"]
    for compose_file in compose_files:
        argv += ["-f", compose_file]
    argv += ["up", "-d", "--force-recreate", "--no-deps", "--no-build"]
    argv += services
    # Compose interpolates every file before merging, so the required-variable
    # guard in docker-compose.dev.yml fires even when the volume list that
    # uses the variable is overridden away. The value itself is never mounted.
    overrides = {"ORACLE_WALLET_HOST_PATH": "D:/server-data/project/Wallet_ETYQPNPJ0L1EP777"}
    return run_command(argv, cwd=workdir, env_overrides=overrides)


def wait_healthy(container: str, timeout_seconds: int) -> str:
    """Wait until a container reports a healthy state.

    Args:
        container: Container name.
        timeout_seconds: Maximum time to wait.

    Returns:
        The final health state string.

    """
    template = "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}"
    deadline = time.monotonic() + timeout_seconds
    state = "unknown"
    while time.monotonic() < deadline:
        code, out, _ = run_command(["docker", "inspect", "-f", template, container])
        if code == 0:
            state = out.strip()
            if state in {"healthy", "running"}:
                return state
        time.sleep(3)
    return state


def container_carries_password(container: str, variable: str, password: str) -> bool:
    """Check whether a container's environment holds the expected credential value.

    The comparison happens inside this process; the value is never printed.

    Args:
        container: Container name.
        variable: Environment variable to read.
        password: Expected value.

    Returns:
        True when the container environment holds the expected value.

    """
    code, out, _ = run_command(["docker", "inspect", "-f", "{{json .Config.Env}}", container])
    if code != 0:
        return False
    try:
        entries = json.loads(out)
    except json.JSONDecodeError:
        return False
    prefix = f"{variable}="
    for entry in entries:
        if entry.startswith(prefix):
            return entry[len(prefix) :] == password
    return False


def recreate_services(args: argparse.Namespace, password: str) -> list[str]:
    """Recreate the app containers and wait for them to report healthy.

    Args:
        args: Parsed command line arguments.
        password: New plaintext password, used to confirm the containers picked it up.

    Returns:
        List of notes describing the outcome per service.

    Raises:
        StepError: If the compose step fails.

    """
    compose_files = [item.strip() for item in args.compose_files.split(",") if item.strip()]
    services = [item.strip() for item in args.services.split(",") if item.strip()]
    code, out, err = compose_up(compose_files, args.compose_workdir, services)
    if code != 0:
        message = f"compose up failed: {(err or out).strip()[:800]}"
        raise StepError(message)

    env_var = env_var_name_for(args.database)
    notes = [f"recreated {', '.join(services)}"]
    for service in services:
        container = f"kbo_{service.replace('-', '_')}"
        state = wait_healthy(container, 180)
        notes.append(f"{service} ({container}): {state}")
        if not container_carries_password(container, env_var, password):
            message = f"{container} does not carry the new {env_var}"
            raise StepError(message)
        notes.append(f"{container} carries the new credential")
    return notes


def apply_change(args: argparse.Namespace, password: str) -> list[str]:
    """Perform the password change and every follow-up step.

    Args:
        args: Parsed command line arguments.
        password: New plaintext password.

    Returns:
        List of notes describing what happened.

    Raises:
        StepError: If any step fails.

    """
    notes: list[str] = []
    alter_role_password(args.container, args.role, password)
    notes.append(f"role {args.role} password altered")

    env_var = env_var_name_for(args.database)
    notes.extend(update_env_file(args.env_file, password, env_var))

    if not verify_login(
        args.container,
        host=args.host,
        port=args.port,
        role=args.role,
        database=args.database,
        password=password,
    ):
        message = "the new password did not authenticate over TCP"
        raise StepError(message)
    notes.append(f"{args.role} authenticated against {args.database} with the new password")

    if not args.no_recreate:
        notes.extend(recreate_services(args, password))
    return notes


def build_parser() -> argparse.ArgumentParser:
    """Build the command line parser."""
    parser = argparse.ArgumentParser(description="Change a PostgreSQL role password and sync local credential stores")
    parser.add_argument("--role", default="bega_prod_user", help="Role whose password is replaced")
    parser.add_argument("--database", default="bega_prod", help="Database the role connects to")
    parser.add_argument("--container", default="kbo_postgres", help="Container holding the PostgreSQL server")
    parser.add_argument("--host", default="100.81.73.13", help="Host used for the post-change verification query")
    parser.add_argument("--port", type=int, default=5432, help="Port used for the post-change verification query")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE, help="Env file to update")
    parser.add_argument("--password-file", type=Path, default=None, help="File whose contents are the new password")
    parser.add_argument(
        "--secret-out",
        type=Path,
        default=DEFAULT_SECRET_DIR / "bega_prod_user.pw",
        help="Where a generated password is stored",
    )
    parser.add_argument(
        "--compose-files",
        default=(
            "docker-compose.dev.yml,"
            "docker-compose.local.yml,"
            "D:/server-data/compose.override.deploy.yml,"
            "D:/server-data/compose.override.credential.yml"
        ),
        help="Comma separated compose files used for the recreate step",
    )
    parser.add_argument(
        "--compose-workdir",
        type=Path,
        default=REPO_ROOT,
        help="Working directory of the compose project",
    )
    parser.add_argument("--services", default="scheduler,api-server", help="Comma separated services to recreate")
    parser.add_argument("--no-recreate", action="store_true", help="Do not recreate the app containers")
    parser.add_argument("--apply", action="store_true", help="Perform the change. Without it nothing is written.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the password change.

    Args:
        argv: Optional argument vector.

    Returns:
        Process exit code.

    """
    args = build_parser().parse_args(argv)

    if not container_is_running(args.container):
        print(f"FAIL: container {args.container} is not running", file=sys.stderr)
        return 2

    try:
        password, secret_path = resolve_password(args, persist=bool(args.apply))
    except StepError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 2

    plan = {
        "role": args.role,
        "database": args.database,
        "container": args.container,
        "env_file": str(args.env_file),
        "env_var": env_var_name_for(args.database),
        "recreate": not args.no_recreate,
        "apply": args.apply,
        "secret_file": str(secret_path) if secret_path is not None else None,
    }
    print(json.dumps(plan, indent=2, ensure_ascii=False))

    if not args.apply:
        print("dry run: nothing was changed (pass --apply to execute)")
        return 0

    try:
        notes = apply_change(args, password)
    except StepError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    for note in notes:
        print(f"OK: {note}")
    if secret_path is not None:
        print(f"OK: generated password stored at {secret_path} (not printed)")
    print("NOTE: update the GitHub Actions secret DATABASE_URL manually to match the new password")
    return 0


if __name__ == "__main__":
    sys.exit(main())
