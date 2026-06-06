import os
import shutil
import subprocess
import sys
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))


def _psql_command(database_url: str, file_path: str) -> tuple[list[str], dict[str, str]]:
    parsed = urlparse(database_url.replace("postgresql+psycopg2://", "postgresql://", 1))
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError(f"Unsupported OCI_DB_URL scheme: {parsed.scheme}")

    psql_path = shutil.which("psql")
    if not psql_path:
        raise RuntimeError("psql executable not found")

    db_name = parsed.path.lstrip("/")
    if not parsed.hostname or not parsed.username or not db_name:
        raise ValueError("OCI_DB_URL must include host, user, and database name")

    env = os.environ.copy()
    if parsed.password:
        env["PGPASSWORD"] = unquote(parsed.password)

    command = [
        psql_path,
        "-h",
        parsed.hostname,
        "-p",
        str(parsed.port or 5432),
        "-U",
        unquote(parsed.username),
        "-d",
        unquote(db_name),
        "-v",
        "ON_ERROR_STOP=1",
        "-f",
        file_path,
    ]
    return command, env


def apply_migration(file_path: str):
    load_dotenv()
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL environment variable not set")
        return

    if not os.path.exists(file_path):
        print(f"❌ Migration file not found: {file_path}")
        return

    print(f"🔌 Connecting to OCI for migration: {file_path}")

    try:
        command, env = _psql_command(oci_url, file_path)
        print(f"📜 Executing SQL from {file_path}...")
        subprocess.run(command, env=env, check=True)
        print("✅ Migration applied successfully.")

    except (Exception, subprocess.CalledProcessError) as e:
        print(f"❌ Migration failed: {e}")
        raise SystemExit(1) from e


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="SQL migration file path")
    args = parser.parse_args()
    apply_migration(args.file)
