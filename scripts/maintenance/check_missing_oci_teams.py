"""
Compare SQLite vs OCI team IDs and report missing codes.

Usage:
    ./venv/bin/python scripts/maintenance/check_missing_oci_teams.py
Ensure OCI_DB_URL (or TARGET_DATABASE_URL) is set in your environment.
"""
from __future__ import annotations

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.engine import create_engine_for_url


def fetch_team_ids(engine, query: str) -> set[str]:
    with engine.connect() as conn:
        return {row[0] for row in conn.execute(text(query))}


def main() -> None:
    load_dotenv()
    sqlite_url = os.getenv("SOURCE_DATABASE_URL", "sqlite:///./data/kbo_dev.db")
    oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not oci_url:
        raise SystemExit("OCI_DB_URL or TARGET_DATABASE_URL must be set")

    sqlite_engine = create_engine_for_url(sqlite_url, disable_sqlite_wal=True)
    oci_engine = create_engine(oci_url, pool_pre_ping=True)

    sqlite_ids = fetch_team_ids(sqlite_engine, "SELECT team_id FROM teams")
    oci_ids = fetch_team_ids(oci_engine, "SELECT team_id FROM teams")

    missing_in_oci = sorted(sqlite_ids - oci_ids)
    missing_in_sqlite = sorted(oci_ids - sqlite_ids)

    if not missing_in_oci and not missing_in_sqlite:
        print("✅ Team IDs are in sync between SQLite and OCI.")
    else:
        if missing_in_oci:
            print("⚠️ Teams missing in OCI:", ", ".join(missing_in_oci))
        if missing_in_sqlite:
            print("ℹ️ Teams present in OCI but not SQLite:", ", ".join(missing_in_sqlite))


if __name__ == "__main__":
    main()
