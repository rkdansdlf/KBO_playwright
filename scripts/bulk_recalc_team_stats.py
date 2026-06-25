"""Bulk recalculate team statistics for all years from 1982 to 2026.
"""

import logging
import subprocess

logger = logging.getLogger(__name__)


def main():
    start_year = 1982
    end_year = 2026

    for year in range(start_year, end_year + 1):
        logger.info("📊 Processing Year %s...", year)
        try:
            # Recalculate batting
            subprocess.run(
                [
                    "venv/bin/python3",
                    "-m",
                    "src.cli.recalc_team_stats",
                    "--year",
                    str(year),
                    "--type",
                    "all",
                    "--save",
                ],
                check=True,
            )
        except subprocess.CalledProcessError:
            logger.exception("❌ Failed to recalculate stats for %s", year)
            continue

    logger.info("✅ Bulk team statistics recalculation complete.")


if __name__ == "__main__":
    main()
