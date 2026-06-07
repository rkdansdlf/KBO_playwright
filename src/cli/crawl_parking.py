from __future__ import annotations

import argparse
import asyncio
import logging

from src.crawlers.parking_crawler import ParkingCrawler

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Crawl stadium parking info")
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--team", type=str, default=None, help="Team code filter")
    args = parser.parse_args(argv)

    crawler = ParkingCrawler()
    result = asyncio.run(crawler.run(save=args.save, team_filter=args.team))
    logger.info(f"[PARKING] Done: {len(result)} lots")


if __name__ == "__main__":
    main()
