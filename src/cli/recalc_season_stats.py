from __future__ import annotations

import argparse
import logging

from src.crawlers.player_batting_all_series_crawler import fallback_batting_from_db
from src.crawlers.player_pitching_all_series_crawler import fallback_pitching_from_db
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.repositories.safe_batting_repository import save_batting_stats_safe

logger = logging.getLogger(__name__)


def _series_list(series: str) -> list[str]:
    if series == "all":
        return ["regular", "wildcard", "semi_playoff", "playoff", "korean_series"]
    return [series]


def _process_batting(year: int, series: str, *, save: bool) -> None:
    logger.info("\n[BATTING] Processing %s %s...", year, series)
    batting_data = fallback_batting_from_db(year, series, reason="Manual CLI Trigger")
    for stat in batting_data:
        stat["source"] = "MANUAL_RECALC"

    if save and batting_data:
        save_batting_stats_safe(batting_data)
    elif not batting_data:
        logger.info("   ℹ️ No batting transactional data found for %s %s.", year, series)


def _process_pitching(year: int, series: str, *, save: bool) -> None:
    logger.info("\n[PITCHING] Processing %s %s...", year, series)
    pitching_data = fallback_pitching_from_db(year, series, reason="Manual CLI Trigger")
    for stat in pitching_data:
        stat.source = "MANUAL_RECALC"

    if save and pitching_data:
        save_pitching_stats_to_db([stat.to_repository_payload() for stat in pitching_data])
    elif not pitching_data:
        logger.info("   ℹ️ No pitching transactional data found for %s %s.", year, series)


def main() -> int:
    parser = argparse.ArgumentParser(description="Recalculate season cumulative stats from transactional game details.")
    parser.add_argument("--year", type=int, required=True, help="Season year")
    parser.add_argument(
        "--series",
        type=str,
        default="regular",
        choices=["regular", "wildcard", "semi_playoff", "playoff", "korean_series", "all"],
        help="Series key",
    )
    parser.add_argument("--type", type=str, default="all", choices=["batting", "pitching", "all"], help="Stat type")
    parser.add_argument("--save", action="store_true", help="Save results to local database")

    args = parser.parse_args()

    logger.info("🚀 Starting Recalculation for %s (Type: %s, Series: %s)", args.year, args.type, args.series)

    for series in _series_list(args.series):
        if args.type in ["batting", "all"]:
            _process_batting(args.year, series, save=args.save)

        if args.type in ["pitching", "all"]:
            _process_pitching(args.year, series, save=args.save)

    logger.info("\n✅ Recalculation task finished.")


if __name__ == "__main__":
    main()
