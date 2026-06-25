"""CLI for collecting congestion data for Jamsil Stadium.

Usage:
    python -m src.cli.crawl_congestion --save
    python -m src.cli.crawl_congestion --game-date 20260603 --save
"""

from __future__ import annotations

import argparse
import asyncio
from typing import TYPE_CHECKING

from src.crawlers.congestion_crawler import CongestionCrawler
from src.utils.date_helpers import parse_date_str

if TYPE_CHECKING:
    from collections.abc import Sequence


async def run(args: argparse.Namespace) -> None:
    """Runs run.

    Args:
        args: Args.

    """
    game_date = None
    if args.game_date:
        game_date = parse_date_str(args.game_date)

    crawler = CongestionCrawler()
    await crawler.run(game_date=game_date, save=args.save)


def build_arg_parser() -> argparse.ArgumentParser:
    """Builds arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="Collect real-time congestion data for Jamsil Stadium area")
    parser.add_argument("--save", action="store_true", help="Save results to database")
    parser.add_argument(
        "--game-date",
        type=str,
        default=None,
        metavar="YYYYMMDD",
        help="Game date (default: today)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Main entry point for this CLI command."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(run(args))


if __name__ == "__main__":  # pragma: no cover
    main()
