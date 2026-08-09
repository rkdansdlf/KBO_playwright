"""CLI 명령: crawl awards history (wikipedia + yagoonara)."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawlers.award_crawler import WIKI_PAGES, AwardCrawler

logger = logging.getLogger(__name__)


async def _run(crawler: AwardCrawler, *, save: bool, types: set[str] | None) -> int:
    """Run the crawl and always close network resources in the same loop.

    Args:
        crawler: The award crawler instance.
        save: Whether to persist records into the DB.
        types: Optional award type filter.

    Returns:
        Number of unique records.

    """
    try:
        return await crawler.run(save=save, types=types)
    finally:
        await crawler.close()


def main(argv: list[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Crawl KBO award history (wikipedia + yagoonara)")

    parser.add_argument("--save", action="store_true", help="Persist parsed awards into the DB")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, do not save")
    parser.add_argument(
        "--type",
        type=str,
        default=None,
        choices=[*WIKI_PAGES, "올스타전MVP", "한국시리즈MVP"],
        help="Award type filter (default: all)",
    )
    args = parser.parse_args(argv)

    should_save = args.save or not args.dry_run
    types = {args.type} if args.type else None

    crawler = AwardCrawler()
    count = asyncio.run(_run(crawler, save=should_save, types=types))
    logger.info("[AWARDS] Done: %s unique records (save=%s)", count, should_save)


if __name__ == "__main__":
    main()
