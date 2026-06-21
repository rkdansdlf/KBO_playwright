"""
CLI tool to recalculate team-level season stats from player season statistics.
Useful for self-healing / rollup recalculations when team stats pages fail.
"""

from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy.exc import SQLAlchemyError

from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.db.engine import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TEAM_RECALC_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)


def _log_dry_run_batting(results: list[dict]) -> None:
    logger.info("  [DRY-RUN] Batting statistics that would be saved:")
    for row in results:
        logger.info(
            "    Team: %-10s (%s) | G: %-3d | AB: %-4d | H: %-4d | AVG: %.3f | OBP: %.3f | SLG: %.3f | OPS: %.3f",
            row.get("team_name", row["team_id"]),
            row["team_id"],
            row["games"],
            row["at_bats"],
            row["hits"],
            row["avg"],
            row["obp"],
            row["slg"],
            row["ops"],
        )


def _log_dry_run_pitching(results: list[dict]) -> None:
    logger.info("  [DRY-RUN] Pitching statistics that would be saved:")
    for row in results:
        logger.info(
            "    Team: %-10s (%s) | G: %-3d | W-L-T: %d-%d-%d | IP: %.1f | ER: %-3d | ERA: %.2f | WHIP: %.2f",
            row.get("team_name", row["team_id"]),
            row["team_id"],
            row["games"],
            row["wins"],
            row["losses"],
            row["ties"],
            row["innings_pitched"],
            row["earned_runs"],
            row["era"],
            row["whip"],
        )


def _run_batting_recalc(aggregator: TeamStatAggregator, season: int, team_id: str | None, dry_run: bool) -> int:
    logger.info("🔄 Recalculating Team Batting Stats for season=%s...", season)
    try:
        results = aggregator.aggregate_batting(season, team_id, dry_run=dry_run)
    except TEAM_RECALC_EXCEPTIONS:
        logger.exception("❌ Failed batting stats rollup for season=%s", season)
        return 1
    else:
        if not results:
            logger.warning("  No batting stats aggregated.")
        else:
            logger.info("  Aggregated %s batting records.", len(results))
            _log_dry_run_batting(results) if dry_run else logger.info(
                "  💾 Upserted %s team batting rows to DB.", len(results)
            )
        return 0


def _run_pitching_recalc(aggregator: TeamStatAggregator, season: int, team_id: str | None, dry_run: bool) -> int:
    logger.info("🔄 Recalculating Team Pitching Stats for season=%s...", season)
    try:
        results = aggregator.aggregate_pitching(season, team_id, dry_run=dry_run)
    except TEAM_RECALC_EXCEPTIONS:
        logger.exception("❌ Failed pitching stats rollup for season=%s", season)
        return 1
    else:
        if not results:
            logger.warning("  No pitching stats aggregated.")
        else:
            logger.info("  Aggregated %s pitching records.", len(results))
            _log_dry_run_pitching(results) if dry_run else logger.info(
                "  💾 Upserted %s team pitching rows to DB.", len(results)
            )
        return 0


def run_recalc(
    season: int,
    team_id: str | None = None,
    dry_run: bool = False,
    batting_only: bool = False,
    pitching_only: bool = False,
) -> int:
    """
    Recalculates team statistics for the given season.
    Returns 0 on success, non-zero on failure.
    """
    if team_id:
        team_id = team_id.upper()

    batting_recalc = not pitching_only
    pitching_recalc = not batting_only

    # 1. Open Session and run rollup
    with SessionLocal() as session:
        aggregator = TeamStatAggregator(session)

        if batting_recalc:
            if _run_batting_recalc(aggregator, season, team_id, dry_run):
                return 1

        if pitching_recalc:
            if _run_pitching_recalc(aggregator, season, team_id, dry_run):
                return 1

    logger.info("✨ Team statistics recalculation completed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Recalculate team cumulative statistics from player stats.")

    # We allow --season as a required argument but support --year/--season flexibly
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--season", type=int, help="Year of the season to recalculate")
    group.add_argument("--year", type=int, help="Deprecated alias for --season")

    parser.add_argument("--team-id", type=str, help="Specific KBO Team ID to recalculate (e.g. LG, OB, SS)")
    parser.add_argument("--team", type=str, dest="team_arg", help="Alias for --team-id")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run rollup in-memory and print results without saving to the database",
    )
    parser.add_argument("--batting-only", action="store_true", help="Recalculate batting stats only")
    parser.add_argument("--pitching-only", action="store_true", help="Recalculate pitching stats only")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    # Maintain backward compatibility with older crawlers calling with --save
    parser.add_argument(
        "--save",
        action="store_true",
        default=True,
        help="Save to DB (ignored as default is true, override with --dry-run)",
    )
    parser.add_argument(
        "--type",
        choices=["batting", "pitching", "all"],
        default="all",
        help="Ignore this legacy argument",
    )
    parser.add_argument("--league", type=str, default="REGULAR", help="Ignore this legacy argument")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    season_val = args.season or args.year
    team_val = args.team_id or args.team_arg

    # Derive restrict flags from both legacy --type and new flags
    bat_only = args.batting_only or (args.type == "batting")
    pit_only = args.pitching_only or (args.type == "pitching")

    sys.exit(
        run_recalc(
            season=season_val,
            team_id=team_val,
            dry_run=args.dry_run,
            batting_only=bat_only,
            pitching_only=pit_only,
        ),
    )


if __name__ == "__main__":
    main()
