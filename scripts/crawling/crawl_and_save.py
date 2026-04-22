"""Deprecated compatibility wrapper for the old integrated crawl pipeline.

The original implementation depended on removed repository/model classes
(`GameRepository`, `GameSchedule`) and duplicated schedule/detail collection
logic that now lives behind the supported CLI entry points.
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Sequence


DEPRECATION_MESSAGE = """
[DEPRECATED] scripts/crawling/crawl_and_save.py is a legacy workflow.

Supported replacements:
  - Schedule collection: python -m src.cli.crawl_schedule --year 2025 --months 3
  - Detail collection:   python -m src.cli.collect_games --year 2025 --month 3
  - Daily operation:     python -m src.cli.run_daily_update --date YYYYMMDD
"""


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the legacy-compatible argument parser."""
    parser = argparse.ArgumentParser(description="Deprecated KBO data collection wrapper")
    parser.add_argument("--season", type=int, default=2024, help="Legacy player season argument; no longer used")
    parser.add_argument("--year", type=int, default=2025, help="Year for schedule collection")
    parser.add_argument("--months", type=str, default="3", help="Months for schedule collection")
    parser.add_argument("--delay", type=float, default=1.2, help="Request delay for delegated schedule collection")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--players-only", action="store_true", help="Legacy player collection mode; no longer supported")
    mode.add_argument("--games-only", action="store_true", help="Delegate schedule collection to src.cli.crawl_schedule")
    return parser


async def _run_schedule_collection(args: argparse.Namespace) -> None:
    """Delegate the still-supported schedule path to the canonical CLI code."""
    from src.cli.crawl_schedule import crawl_schedule

    await crawl_schedule(
        argparse.Namespace(
            year=args.year,
            months=args.months,
            delay=args.delay,
        )
    )


async def run(argv: Sequence[str] | None = None) -> int:
    """Run the deprecated wrapper and return a process exit code."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    print(DEPRECATION_MESSAGE.strip())

    if args.games_only:
        await _run_schedule_collection(args)
        return 0

    print(
        "\nThe legacy player collection path was removed because it writes through "
        "obsolete models. This wrapper intentionally does not run a partial "
        "default pipeline."
    )
    print("Use --games-only to delegate schedule collection, or run the supported CLI commands above.")
    return 2


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous script entry point."""
    return asyncio.run(run(argv))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
