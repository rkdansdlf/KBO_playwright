"""CLI command to recalculate player milestones and situational splits."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST
from src.db.engine import get_db_session
from src.services.stat_recalculator import StatRecalculator

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> None:
    """Run recalculation of milestones and splits.

    Args:
        args: Positional CLI arguments.

    """
    season = args.season or datetime.now(tz=KST).year

    with get_db_session() as session:
        recalculator = StatRecalculator(session)
        updated_m = recalculator.recalc_player_milestones(season=season)
        updated_s = recalculator.recalc_player_splits(season=season)
        logger.info("StatRecalculator updated %d milestones and %d splits for season %d.", updated_m, updated_s, season)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI argument parser."""
    parser = argparse.ArgumentParser(description="Recalculate player milestones & splits")
    parser.add_argument("--season", type=int, default=None, help="Season year (default: current)")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """CLI entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    run(args)


if __name__ == "__main__":  # pragma: no cover
    main()
