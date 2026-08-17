"""CLI command to crawl KBO player upcoming milestones."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST
from src.crawlers.milestone_crawler import MilestoneCrawler
from src.db.engine import get_db_session
from src.repositories.milestone_repository import MilestoneRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> None:
    """Run milestone crawler.

    Args:
        args: Command line arguments.

    """
    season = args.season or datetime.now(tz=KST).year
    crawler = MilestoneCrawler()
    items = await crawler.crawl_upcoming_milestones(season=season)
    logger.info("Crawled %d milestone items for season %d.", len(items), season)

    if args.save and items:
        with get_db_session() as session:
            repo = MilestoneRepository(session)
            saved_count = 0
            for item in items:
                repo.save_milestone(item)
                saved_count += 1
            logger.info("Saved %d milestone records to DB.", saved_count)
    else:
        for item in items[:5]:
            logger.info("Sample milestone: %s", item)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build argument parser.

    Returns:
        ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(description="KBO Milestone Crawler")
    parser.add_argument("--save", action="store_true", help="Save crawled data to DB")
    parser.add_argument("--season", type=int, default=None, help="Target season (default: current year)")
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
