"""Hydrate fresh local runtime cache from OCI."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from src.db.engine import SessionLocal, create_engine_for_url
from src.sync.runtime_hydrator import RuntimeHydrator
from src.utils.date_helpers import parse_date_str

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
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
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Send Telegram/Slack notification upon successful hydration.",
    )
    parser.add_argument(
        "--quarantine-dir",
        type=str,
        help="Quarantine directory path if this hydration is a recovery after DB quarantine.",
    )
    args = parser.parse_args(argv)

    if not args.source_url:
        msg = "OCI_DB_URL or --source-url is required"
        raise SystemExit(msg)

    target_date = parse_date_str(args.date) if args.date else None
    source_engine = create_engine_for_url(args.source_url, disable_sqlite_wal=True)
    source_session_factory = sessionmaker(bind=source_engine, autoflush=False, autocommit=False, expire_on_commit=False)

    with source_session_factory() as source_session, SessionLocal() as target_session:
        # Disable FK constraints during hydration to allow out-of-order inserts
        target_session.execute(text("PRAGMA foreign_keys=OFF"))
        try:
            hydrator = RuntimeHydrator(source_session, target_session)
            summary = hydrator.hydrate_year(
                args.year,
                target_date=target_date,
                preserve_aliases=args.preserve_aliases,
            )
        finally:
            target_session.execute(text("PRAGMA foreign_keys=ON"))

    logger.info("✅ Hydrated runtime cache for %s: %s", args.year, summary)

    is_quarantine_recovery = bool(os.getenv("SQLITE_GUARD_QUARANTINED") == "1" or args.quarantine_dir)
    quarantine_dir = args.quarantine_dir
    guard_path = Path("/tmp/sqlite_integrity_guard.json")  # noqa: S108
    if not quarantine_dir and guard_path.exists():
        try:
            import json

            with guard_path.open(encoding="utf-8") as f:
                guard_data = json.load(f)
                if guard_data.get("status") == "quarantined":
                    is_quarantine_recovery = True
                    quarantine_dir = guard_data.get("quarantine_dir")
        except (OSError, json.JSONDecodeError):
            pass

    if args.notify or is_quarantine_recovery:
        from src.utils.alerting import SlackWebhookClient

        SlackWebhookClient.send_hydration_alert(
            args.year,
            summary,
            quarantine_dir=quarantine_dir,
            is_quarantine_recovery=is_quarantine_recovery,
        )

    source_engine.dispose()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
