"""CLI 명령: crawl seat sections."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawlers.seat_crawler import SeatCrawler

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Crawl stadium seat section info")

    parser.add_argument("--save", action="store_true")
    parser.add_argument("--team", type=str, default=None, help="Team code filter")
    args = parser.parse_args(argv)

    crawler = SeatCrawler()
    result = asyncio.run(crawler.run(save=args.save, team_filter=args.team))
    logger.info("[SEAT] Done: %s sections", len(result))


if __name__ == "__main__":
    main()
