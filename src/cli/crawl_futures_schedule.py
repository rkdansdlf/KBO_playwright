"""CLI command to crawl Futures League schedule and standings."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST
from src.crawlers.futures_schedule_crawler import FuturesScheduleCrawler
from src.db.engine import get_db_session
from src.repositories.futures_schedule_repository import FuturesScheduleRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> None:
    """Run futures schedule crawler.

    Args:
        args: Command line arguments.

    """
    now = datetime.now(tz=KST)
    year = args.year or now.year
    month = args.month or now.month

    crawler = FuturesScheduleCrawler()
    items = await crawler.crawl_futures_schedule(year=year, month=month)
    logger.info("Crawled %d futures schedule items for %d-%02d.", len(items), year, month)

    if args.save and items:
        with get_db_session() as session:
            repo = FuturesScheduleRepository(session)
            saved_count = 0
            for item in items:
                repo.save_game_schedule(item)
                saved_count += 1
            logger.info("Saved %d futures schedule records to DB.", saved_count)
    else:
        for item in items[:5]:
            logger.info("Sample futures schedule: %s", item)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build argument parser.

    Returns:
        ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(description="KBO Futures League Schedule Crawler")
    parser.add_argument("--save", action="store_true", help="Save crawled data to DB")
    parser.add_argument("--year", type=int, default=None, help="Target year")
    parser.add_argument("--month", type=int, default=None, help="Target month")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entrypoint.

    Args:
        argv: Optional command line arguments.

    """
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
