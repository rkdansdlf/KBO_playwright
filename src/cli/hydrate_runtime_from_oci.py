"""Hydrate fresh local runtime cache from OCI."""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Sequence
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker

from src.db.engine import SessionLocal, create_engine_for_url
from src.sync.runtime_hydrator import RuntimeHydrator

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Hydrate local runtime SQLite cache from OCI/Postgres")
    parser.add_argument("--source-url", type=str, default=os.getenv("OCI_DB_URL"), help="OCI/Postgres source URL")
    parser.add_argument("--year", type=int, required=True, help="Season year to hydrate")
    parser.add_argument("--date", type=str, help="Optional target date YYYYMMDD for recent roster window")
    parser.add_argument(
        "--preserve-aliases",
        action="store_true",
        help="Preserve existing local game_id_aliases for the hydrated year instead of replacing them from OCI.",
    )
    args = parser.parse_args(argv)

    if not args.source_url:
        msg = "OCI_DB_URL or --source-url is required"
        raise SystemExit(msg)

    target_date = datetime.strptime(args.date, "%Y%m%d").date() if args.date else None
    source_engine = create_engine_for_url(args.source_url, disable_sqlite_wal=True)
    SourceSession = sessionmaker(bind=source_engine, autoflush=False, autocommit=False, expire_on_commit=False)

    with SourceSession() as source_session, SessionLocal() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        summary = hydrator.hydrate_year(
            args.year,
            target_date=target_date,
            preserve_aliases=args.preserve_aliases,
        )

    logger.info("✅ Hydrated runtime cache for %s: %s", args.year, summary)
    source_engine.dispose()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
