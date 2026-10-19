"""CLI 명령: crawl seat sections."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import TYPE_CHECKING

from src.crawlers.seat_crawler import SeatCrawler

if TYPE_CHECKING:
    from src.crawlers.sweep_outcome import CrawlSweepOutcome

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> CrawlSweepOutcome | None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    Returns:
        What the sweep did. The entrypoint returns it rather than nothing so an
        in-process caller such as ``crawl_p1p2_data_job`` can report what was
        collected instead of assuming that a returned call means collected data.
        ``None`` only if the crawler never completed a run, which the caller
        treats as unreported rather than as success.

    """
    parser = argparse.ArgumentParser(description="Crawl stadium seat section info")

    parser.add_argument("--save", action="store_true")
    parser.add_argument("--team", type=str, default=None, help="Team code filter")
    args = parser.parse_args(argv)

    crawler = SeatCrawler()
    result = asyncio.run(crawler.run(save=args.save, team_filter=args.team))
    logger.info("[SEAT] Done: %s sections", len(result))
    return crawler.last_run_outcome


if __name__ == "__main__":
    main()
