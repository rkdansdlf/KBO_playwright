"""CLI entry point for historical (2001-2009) KBO game detail backfill."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from src.db.engine import SessionLocal
from src.services.historical_detail_backfill_service import HistoricalDetailBackfillService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backfill_historical_details")


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for historical detail backfill CLI."""
    parser = argparse.ArgumentParser(
        description="Backfill missing historical (2001-2009) game details and boxscores.",
    )
    year_group = parser.add_mutually_exclusive_group(required=True)
    year_group.add_argument("--year", type=int, help="Single season year to backfill (e.g. 2009)")
    year_group.add_argument("--start-year", type=int, help="Start year for range backfill (e.g. 2001)")

    parser.add_argument("--end-year", type=int, help="End year for range backfill (e.g. 2009)")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of games to backfill per season")
    parser.add_argument("--delay", type=float, default=1.0, help="Throttle delay between requests in seconds")
    parser.add_argument("--save", action="store_true", help="Commit extracted data to database (default: dry-run)")
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run browser in headless mode",
    )
    parser.add_argument("--json", action="store_true", help="Output summary in JSON format")

    return parser


def main(argv: list[str] | None = None) -> int:
    """Execute historical detail backfill pipeline."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.year:
        start_year = args.year
        end_year = args.year
    else:
        start_year = args.start_year
        end_year = args.end_year or start_year

    if start_year > end_year:
        logger.error("❌ start-year (%s) cannot exceed end-year (%s)", start_year, end_year)
        return 1

    dry_run = not args.save
    if dry_run:
        logger.info("🔍 Running in DRY-RUN mode. Pass --save to commit to database.")
    else:
        logger.info("💾 Running in SAVE mode. Extracted boxscores will be committed to DB.")

    session = SessionLocal()
    try:
        service = HistoricalDetailBackfillService(session=session, request_delay=args.delay)
        results = service.run_backfill(
            start_year=start_year,
            end_year=end_year,
            limit_per_season=args.limit,
            dry_run=dry_run,
            headless=args.headless,
        )

        if args.json:
            print(json.dumps([r.to_dict() for r in results], indent=2, ensure_ascii=False))  # noqa: T201
        else:
            print("\n" + "=" * 50)  # noqa: T201
            print("📊 Historical Detail Backfill Summary")  # noqa: T201
            print("=" * 50)  # noqa: T201
            for r in results:
                print(  # noqa: T201
                    f"Season {r.year}: Missing={r.total_missing}, Attempted={r.attempted}, "
                    f"Saved/Validated={r.saved}, ValidationSkipped={r.skipped_validation}, Failed={r.failed}"
                )
            print("=" * 50 + "\n")  # noqa: T201

    except Exception:
        logger.exception("🔥 Fatal error during historical detail backfill")
        return 1
    finally:
        session.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
