"""KBO Advanced Daily Data Update Orchestrator.

Fetch fielding, baserunning, and team-level cumulative stats.

The individual steps live in :mod:`src.cli.pipelines.advanced_daily_steps`, which the
canonical scheduler's daily pipeline shares. This module keeps the CLI entry point and
the ordered orchestration that GitHub Actions runs.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from src.cli.pipelines.advanced_daily_steps import (
    ADVANCED_STEP_EXCEPTIONS,
    aggregate_team_defense_step,
    crawl_baserunning_step,
    crawl_fielding_step,
    crawl_team_batting_step,
    crawl_team_pitching_step,
    filter_player_rows,
    rebuild_rankings_step,
    run_step,
)

__all__ = [
    "ADVANCED_STEP_EXCEPTIONS",
    "aggregate_team_defense_step",
    "crawl_baserunning_step",
    "crawl_fielding_step",
    "crawl_team_batting_step",
    "crawl_team_pitching_step",
    "filter_player_rows",
    "main",
    "rebuild_rankings_step",
    "run_advanced_update",
    "run_step",
]

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


async def run_advanced_update(
    year: int,
    *,
    headless: bool = True,
) -> None:
    """Run advanced.

    Args:
        year: Season year.
        headless: Whether to run the browser in headless mode.

    """
    logger.info("\n%s", "=" * 60)

    logger.info("🚀 KBO Advanced Daily Sync Started for Year: %s", year)
    logger.info("%s", "=" * 60)

    any_error = False

    any_error |= await run_step(
        "🛡️ Step 1: Crawling Fielding Stats...",
        "Error crawling fielding stats",
        lambda: crawl_fielding_step(year),
    )
    any_error |= await run_step(
        "🏃 Step 2: Crawling Baserunning Stats...",
        "Error crawling baserunning stats",
        lambda: crawl_baserunning_step(year),
    )
    any_error |= await run_step(
        "🏏 Step 3: Crawling Team Batting Stats...",
        "Error crawling team batting stats",
        lambda: crawl_team_batting_step(year, headless=headless),
    )
    any_error |= await run_step(
        "⚾ Step 4: Crawling Team Pitching Stats...",
        "Error crawling team pitching stats",
        lambda: crawl_team_pitching_step(year, headless=headless),
    )
    any_error |= await run_step(
        "🏰 Step 5: Aggregating Team Fielding & Baserunning...",
        "Error aggregating team defense stats",
        lambda: aggregate_team_defense_step(year),
    )
    any_error |= await run_step(
        "🏷️ Step 6: Recalculating Stat Rankings...",
        "Error recalculating rankings",
        lambda: rebuild_rankings_step(year),
    )

    logger.info("\n%s", "=" * 60)
    logger.info("🏁 Advanced Daily Sync Finished for %s", year)
    logger.info("%s\n", "=" * 60)

    if any_error:
        msg = f"Advanced Daily Sync finished with errors for {year}"
        raise RuntimeError(msg)


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Advanced Daily Data Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run with browser UI")

    args = parser.parse_args()

    year = args.year or datetime.now(KST).year
    asyncio.run(run_advanced_update(year, headless=args.headless))
    return 0


if __name__ == "__main__":
    main()
