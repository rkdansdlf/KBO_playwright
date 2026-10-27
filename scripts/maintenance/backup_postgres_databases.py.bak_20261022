r"""Nightly PostgreSQL backup to D:\kbo_backup with retention.

Dumps bega_prod (primary), bega_dev, and kbo_rag through the kbo_postgres
container (which has pg_dump and reaches both servers), then copies the
artifacts to D:\\kbo_backup. Keeps the newest ``--keep`` dumps per database
(default 3).

Credentials come from ``.env`` (DATABASE_URL, LOCAL_PG_URL, PGVECTOR_URL) and
are passed to pg_dump via a transient PGPASSWORD environment variable.

Usage:
    python scripts/maintenance/backup_postgres_databases.py
    python scripts/maintenance/backup_postgres_databases.py --keep 5 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

CONTAINER_TMP = "/tmp"  # noqa: S108 - container-side scratch dir (Linux container).

BACKUP_DIR = Path("D:/kbo_backup")

logger = logging.getLogger("pg_backup")


#: Backup label -> the ``.env`` key holding its container-perspective URL.
URL_ENV_KEYS = {
    "bega_prod": "DATABASE_URL",
    "bega_dev": "LOCAL_PG_URL_DOCKER",
    "kbo_rag": "PGVECTOR_URL_DOCKER",
}


def _load_env() -> dict[str, str | None]:
    """Load database URLs from the repository .env file.

    A missing key yields ``None`` for that label instead of raising. One absent URL used to abort
    the whole run before any dump was taken, which silently stopped every backup for days.
    """
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
    # Container-perspective URLs: the dump runner is itself a container.
    return {label: os.environ.get(key) or None for label, key in URL_ENV_KEYS.items()}


def _split(url: str) -> tuple[str, str, str, str]:
    """Split a SQLAlchemy URL into user, password, host:port, and database."""
    parts = urlsplit(url)
    host = parts.hostname or "127.0.0.1"
    port = parts.port or 5432
    user = parts.username or "postgres"
    password = parts.password or ""
    database = (parts.path or "/postgres").lstrip("/")
    return user, password, f"{host}:{port}", database


def _dump(label: str, url: str, stamp: str, *, dry_run: bool) -> Path | None:
    """Dump one database via the local pg container; return the artifact path."""
    user, password, hostport, database = _split(url)
    host, port = hostport.split(":")
    if host in {"127.0.0.1", "localhost"}:
        # Loopback from the host shell means "the pg container itself" here.
        host, port = "/var/run/postgresql", "5432"
    artifact = BACKUP_DIR / f"{label}_{stamp}.dump"
    container_path = f"{CONTAINER_TMP}/{artifact.name}"
    dump_cmd = [
        "docker",
        "exec",
        "-e",
        f"PGPASSWORD={password}",
        "kbo_postgres",
        "pg_dump",
        "-h",
        host,
        "-p",
        port,
        "-U",
        user,
        "-d",
        database,
        "-Fc",
        "-f",
        container_path,
    ]
    cp_cmd = ["docker", "cp", f"kbo_postgres:{container_path}", str(artifact)]
    rm_cmd = ["docker", "exec", "kbo_postgres", "rm", container_path]
    if dry_run:
        logger.info("DRY-RUN %s -> %s", label, artifact)
        return None
    subprocess.run(dump_cmd, check=True, capture_output=True)
    subprocess.run(cp_cmd, check=True, capture_output=True)
    subprocess.run(rm_cmd, check=True, capture_output=True)
    if artifact.stat().st_size == 0:
        artifact.unlink()
        msg = f"{label} dump is empty"
        raise RuntimeError(msg)
    logger.info("%s -> %s (%.1f MB)", label, artifact, artifact.stat().st_size / 1048576)
    return artifact


def _dated_dumps(label: str) -> list[Path]:
    """Return the non-empty ``<label>_YYYYMMDD.dump`` files, oldest first.

    Only files this script produces count. Hand-made dumps such as ``bega_prod_after_recovery_*`` or
    ``kbo_rag_bega_dev_*`` share the prefix but are never retention candidates, and empty files do not
    count as backups so they cannot displace a real one.
    """
    pattern = re.compile(rf"^{re.escape(label)}_\d{{8}}\.dump$")
    return sorted(p for p in BACKUP_DIR.iterdir() if pattern.match(p.name) and p.stat().st_size > 0)


def _enforce_retention(label: str, keep: int, *, dry_run: bool) -> None:
    """Delete dated dumps beyond the newest ``keep`` for a label."""
    artifacts = _dated_dumps(label)
    for stale in artifacts[:-keep] if len(artifacts) > keep else []:
        if dry_run:
            logger.info("DRY-RUN rm %s", stale)
        else:
            stale.unlink()
            logger.info("rm %s", stale)


def main(argv: list[str] | None = None) -> int:
    """Run the nightly backup for all configured databases."""
    parser = argparse.ArgumentParser(description="Back up PostgreSQL databases to D:/kbo_backup.")
    parser.add_argument("--keep", type=int, default=3, help="Dumps to keep per database.")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without writing.")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    except OSError:
        logger.exception("Cannot create %s", BACKUP_DIR)
        return 2

    urls = _load_env()

    stamp = datetime.now().strftime("%Y%m%d")
    failed = 0
    for label, url in urls.items():
        if not url:
            logger.error("%s skipped: %s is not set in .env", label, URL_ENV_KEYS[label])
            failed += 1
            continue
        try:
            _dump(label, url, stamp, dry_run=args.dry_run)
            _enforce_retention(label, args.keep, dry_run=args.dry_run)
        except (subprocess.CalledProcessError, RuntimeError, OSError) as exc:
            # NOTE: log the type/status only -- a CalledProcessError message contains the dump command line.
            status = getattr(exc, "returncode", type(exc).__name__)
            logger.error("%s backup failed (%s)", label, status)  # noqa: TRY400
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
