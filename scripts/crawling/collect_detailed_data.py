"""Deprecated compatibility wrapper for the old detailed-data collector.

The original script used a removed pending-game queue and stale repository
classes. Keep this file import-safe so old automation fails with actionable
guidance instead of crashing before the deprecation notice is printed.
"""
from __future__ import annotations

import argparse
from typing import Sequence


DEPRECATION_MESSAGE = """
[DEPRECATED] scripts/crawling/collect_detailed_data.py is a legacy workflow.

Supported replacements:
  - Completed game details: python -m src.cli.collect_games --year 2025 --month 3
  - Single date detail:     python -m src.cli.crawl_game_details --date YYYYMMDD
  - Daily finalization:     python -m src.cli.run_daily_update --date YYYYMMDD
  - OCI sync:               python -m src.cli.sync_oci --truncate
"""


def build_arg_parser() -> argparse.ArgumentParser:
    """Build a parser that accepts the legacy flags for clear deprecation output."""
    parser = argparse.ArgumentParser(description="Deprecated detailed KBO data collection wrapper")
    parser.add_argument("--players", action="store_true", help="Legacy player profile mode; no longer supported")
    parser.add_argument("--games", action="store_true", help="Legacy pending-game mode; no longer supported")
    parser.add_argument("--all", action="store_true", help="Legacy combined mode; no longer supported")
    parser.add_argument("--limit", type=int, default=10, help="Legacy limit argument; no longer used")
    parser.add_argument("--sync", action="store_true", help="Legacy Supabase sync flag; no longer used")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Print deprecation guidance and return a non-zero exit code."""
    build_arg_parser().parse_args(argv)

    print(DEPRECATION_MESSAGE.strip())
    print(
        "\nThis script cannot safely infer year/month targets from the removed "
        "pending-game queue. Use the supported CLI command that matches the "
        "collection window you want to run."
    )
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
