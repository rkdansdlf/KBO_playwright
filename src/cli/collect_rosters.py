"""Daily Roster Collector CLI."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.db.engine import SessionLocal
from src.repositories.team_repository import TeamRepository

logger = logging.getLogger(__name__)

ROSTER_SAVE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


def save_chunk(chunk: list[dict[str, Any]]) -> None:
    """
    Save chunk.

    Args:
        chunk: Chunk.
        chunk: Chunk.

    """
    session = SessionLocal()

    try:
        repo = TeamRepository(session)
        count = repo.save_daily_rosters(chunk)
        logger.info("   💾 Saved chunk of %s records (New/Updated: %s)", len(chunk), count)
    except ROSTER_SAVE_EXCEPTIONS:
        logger.exception("   ⚠️ Error saving chunk")
    finally:
        session.close()


async def collect_rosters(year: int, month: int | None = None) -> None:
    """
    Handle the collect rosters operation.

    Args:
        year: Season year.
        month: Month.
        year: Season year.
        month: Month number (1-12).

    """
    crawler = DailyRosterCrawler()

    # Define date range
    if month:
        start_date = date(year, month, 1)
        # End date: start of next month - 1 day
        end_date = date(year, 12, 31) if month == 12 else date(year, month + 1, 1) - timedelta(days=1)
    else:
        # Full year (Regular season roughly Mar-Nov)
        start_date = date(year, 3, 1)
        end_date = date(year, 11, 30)

    logger.info("🗓️  Collecting Daily Rosters: %s ~ %s", start_date, end_date)

    await crawler.crawl_date_range(
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        save_callback=save_chunk,
    )

    logger.info("✅ Finished Roster Collection for %s%s", year, f"-{month}" if month else "")


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Collect Daily Rosters")
    parser.add_argument("--year", type=int, required=True, help="Target Year")
    parser.add_argument("--month", type=int, help="Target Month (Optional)")
    args = parser.parse_args()

    asyncio.run(collect_rosters(args.year, args.month))


if __name__ == "__main__":
    main()
