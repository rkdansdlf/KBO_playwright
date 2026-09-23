r"""Nightly PostgreSQL backup to D:\kbo_backup with retention.

Dumps the local kbo (+kbo_rag) databases and the Tailscale bega_prod
database through the kbo_pg_local container (which has pg_dump and
reaches both servers), then copies the artifacts to D:\\kbo_backup.
Keeps the newest ``--keep`` dumps per database (default 3).

Credentials come from ``.env`` (LOCAL_PG_URL, DATABASE_URL) and are
passed to pg_dump via a transient PGPASSWORD environment variable.

Usage:
    python scripts/maintenance/backup_postgres_databases.py
    python scripts/maintenance/backup_postgres_databases.py --keep 5 --dry-run

Register on Windows Task Scheduler (daily 04:00):
    schtasks /create /tn KBO_PG_Backup /tr "C:\\Project\\KBO_playwright\\.venv\\Scripts\\python.exe C:\\Project\\KBO_playwright\\scripts\\maintenance\\backup_postgres_databases.py" /sc DAILY /st 04:00
"""

from __future__ import annotations

import argparse
import logging
import os
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


def _load_env() -> dict[str, str]:
    """Load database URLs from the repository .env file."""
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=PROJECT_ROOT / ".env")
    return {
        "local_kbo": os.environ["LOCAL_PG_URL"],
        "local_rag": os.environ["PGVECTOR_URL"],
        "tailscale": os.environ["DATABASE_URL"],
    }


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
        "kbo_pg_local",
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
    cp_cmd = ["docker", "cp", f"kbo_pg_local:{container_path}", str(artifact)]
    rm_cmd = ["docker", "exec", "kbo_pg_local", "rm", container_path]
    if dry_run:
        logger.info("DRY-RUN %s -> %s", label, artifact)
        return None
    subprocess.run(dump_cmd, check=True, capture_output=True)
    subprocess.run(cp_cmd, check=True, capture_output=True)
    subprocess.run(rm_cmd, check=True, capture_output=True)
    logger.info("%s -> %s (%.1f MB)", label, artifact, artifact.stat().st_size / 1048576)
    return artifact


def _enforce_retention(label: str, keep: int, *, dry_run: bool) -> None:
    """Delete dumps beyond the newest ``keep`` artifacts for a label."""
    artifacts = sorted(BACKUP_DIR.glob(f"{label}_*.dump"))
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

    try:
        urls = _load_env()
    except KeyError:
        logger.exception("Missing URL in .env")
        return 2

    stamp = datetime.now().strftime("%Y%m%d")
    failed = 0
    for label, url in (("kbo", urls["local_kbo"]), ("kbo_rag", urls["local_rag"]), ("bega_prod", urls["tailscale"])):
        try:
            _dump(label, url, stamp, dry_run=args.dry_run)
            _enforce_retention(label, args.keep, dry_run=args.dry_run)
        except subprocess.CalledProcessError as exc:
            # NOTE: log status only — the exception text contains the db command line.
            logger.error("%s backup failed (rc=%s)", label, exc.returncode)  # noqa: TRY400
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
