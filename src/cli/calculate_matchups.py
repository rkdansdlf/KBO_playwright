"""CLI 명령: calculate matchups."""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.services.matchup_engine import MatchupEngine

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

MATCHUP_CALC_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, OSError)


def batch_calculate_matchups(years: list[int], *, sync_oci: bool = False) -> None:
    """Runs the MatchupEngine for a range of years to compute BvP and Splits."""
    engine = MatchupEngine()

    for year in years:
        try:
            engine.execute_all(year)
        except MATCHUP_CALC_EXCEPTIONS:
            logger.exception("⚠️ Failed to calculate matchups for %s", year)

    if sync_oci:
        logger.info("🚀 Syncing Matchups to OCI...")
        import os

        from src.cli.sync_oci import OCISync

        target_url = os.getenv("OCI_DB_URL")
        if target_url:
            with SessionLocal() as session:
                syncer = OCISync(target_url, session)
                syncer.sync_matchups()
            logger.info("✅ Sync complete.")
        else:
            logger.warning("⚠️ OCI_DB_URL not set, skipping sync.")


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Calculate Matchup and Split matrices.")
    parser.add_argument("--years", type=str, default="2020-2026")
    parser.add_argument("--sync", action="store_true", help="Sync results to OCI")
    args = parser.parse_args(argv)

    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]

    batch_calculate_matchups(target_years, sync_oci=args.sync)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
