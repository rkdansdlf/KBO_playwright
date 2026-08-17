"""CLI 명령: calculate matchups."""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.services.matchup_engine import MatchupEngine

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

MATCHUP_CALC_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


def batch_calculate_matchups(years: list[int]) -> None:
    """Run the MatchupEngine for a range of years to compute BvP and Splits.

    Args:
        years: Years.

    """
    engine = MatchupEngine()

    for year in years:
        try:
            engine.execute_all(year)
        except MATCHUP_CALC_EXCEPTIONS:
            logger.exception("⚠️ Failed to calculate matchups for %s", year)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Calculate Matchup and Split matrices.")

    parser.add_argument("--years", type=str, default="2020-2026")
    args = parser.parse_args(argv)

    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]

    batch_calculate_matchups(target_years)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
