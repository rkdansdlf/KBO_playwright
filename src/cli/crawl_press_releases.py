"""CLI command to crawl KBO press releases and administrative notices."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import TYPE_CHECKING

from src.crawlers.press_release_crawler import PressReleaseCrawler
from src.db.engine import get_db_session
from src.repositories.press_release_repository import KboPressReleaseRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> None:
    """Run press release crawler.

    Args:
        args: Command line arguments.

    """
    crawler = PressReleaseCrawler()
    items = await crawler.crawl_press_releases(max_pages=args.max_pages)
    logger.info("Crawled %d press release items.", len(items))

    if args.save and items:
        with get_db_session() as session:
            repo = KboPressReleaseRepository(session)
            saved_count = 0
            for item in items:
                repo.save_press_release(item)
                saved_count += 1
            logger.info("Saved %d press release records to database.", saved_count)
    else:
        for item in items[:5]:
            logger.info("Sample press release: %s", item)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build argument parser.

    Returns:
        ArgumentParser instance.

    """
    parser = argparse.ArgumentParser(description="KBO Press Releases Crawler")
    parser.add_argument("--save", action="store_true", help="Save crawled data to DB")
    parser.add_argument("--max-pages", type=int, default=1, help="Max pages to crawl (default: 1)")
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
