"""Hydrate fresh local runtime cache from OCI."""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import Sequence

from sqlalchemy.orm import sessionmaker

from src.db.engine import SessionLocal, create_engine_for_url
from src.sync.runtime_hydrator import RuntimeHydrator


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Hydrate local runtime SQLite cache from OCI/Postgres")
    parser.add_argument("--source-url", type=str, default=os.getenv("OCI_DB_URL"), help="OCI/Postgres source URL")
    parser.add_argument("--year", type=int, required=True, help="Season year to hydrate")
    parser.add_argument("--date", type=str, help="Optional target date YYYYMMDD for recent roster window")
    args = parser.parse_args(argv)

    if not args.source_url:
        raise SystemExit("OCI_DB_URL or --source-url is required")

    target_date = datetime.strptime(args.date, "%Y%m%d").date() if args.date else None
    source_engine = create_engine_for_url(args.source_url, disable_sqlite_wal=True)
    SourceSession = sessionmaker(bind=source_engine, autoflush=False, autocommit=False, expire_on_commit=False)

    with SourceSession() as source_session, SessionLocal() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        summary = hydrator.hydrate_year(args.year, target_date=target_date)

    print(f"✅ Hydrated runtime cache for {args.year}: {summary}")
    source_engine.dispose()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
