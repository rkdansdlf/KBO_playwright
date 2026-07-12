"""CLI for measuring transit times from transit hubs to Jamsil Stadium.

Usage:
    python -m src.cli.crawl_transit_time --save
    python -m src.cli.crawl_transit_time --game-date 20260603 --save
    python -m src.cli.crawl_transit_time --origin 잠실역_2호선_7번출구

"""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import TYPE_CHECKING

from src.crawlers.transit_time_crawler import JAMSIL_ORIGINS, TransitTimeCrawler
from src.utils.date_helpers import parse_date_str

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def run(args: argparse.Namespace) -> None:
    """Run run.

    Args:
        args: Positional arguments to pass through.
        args: Args.

    """
    game_date = None

    if args.game_date:
        game_date = parse_date_str(args.game_date)

    origins = JAMSIL_ORIGINS
    if args.origin:
        origins = [o for o in JAMSIL_ORIGINS if o["label"] == args.origin]
        if not origins:
            logger.info("Unknown origin: %s", args.origin)
            logger.info("Available: %s", [o["label"] for o in JAMSIL_ORIGINS])
            return

    crawler = TransitTimeCrawler(origins=origins)
    await crawler.run(game_date=game_date, save=args.save)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="Measure transit times from nearby stations to Jamsil Stadium")

    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument(
        "--game-date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="Game date (default: today)",
    )
    parser.add_argument(
        "--origin",
        type=str,
        default=None,
        help="Specific origin label to measure (default: all configured origins)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)
    asyncio.run(run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
