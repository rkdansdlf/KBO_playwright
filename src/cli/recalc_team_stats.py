"""
CLI tool to recalculate team-level season stats from player season statistics.
Useful for self-healing / rollup recalculations when team stats pages fail.
"""

import argparse
import logging
import sys

from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.db.engine import SessionLocal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_recalc(
    season: int,
    team_id: str | None = None,
    dry_run: bool = False,
    batting_only: bool = False,
    pitching_only: bool = False,
    verbose: bool = False,
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

        # A. Batting Recalculation
        if batting_recalc:
            logger.info(f"🔄 Recalculating Team Batting Stats for season={season}...")
            try:
                batting_results = aggregator.aggregate_batting(season, team_id, dry_run=dry_run)

                if not batting_results:
                    logger.warning("  No batting stats aggregated.")
                else:
                    logger.info(f"  Aggregated {len(batting_results)} batting records.")

                    if dry_run:
                        logger.info("  [DRY-RUN] Batting statistics that would be saved:")
                        for r in batting_results:
                            logger.info(
                                "    Team: %-10s (%s) | G: %-3d | AB: %-4d | H: %-4d | AVG: %.3f | OBP: %.3f | SLG: %.3f | OPS: %.3f",
                                r.get("team_name", r["team_id"]),
                                r["team_id"],
                                r["games"],
                                r["at_bats"],
                                r["hits"],
                                r["avg"],
                                r["obp"],
                                r["slg"],
                                r["ops"],
                            )
                    else:
                        logger.info(f"  💾 Upserted {len(batting_results)} team batting rows to DB.")

            except Exception as e:
                logger.exception(f"❌ Failed batting stats rollup for season={season}: {e}")
                return 1

        # B. Pitching Recalculation
        if pitching_recalc:
            logger.info(f"🔄 Recalculating Team Pitching Stats for season={season}...")
            try:
                pitching_results = aggregator.aggregate_pitching(season, team_id, dry_run=dry_run)

                if not pitching_results:
                    logger.warning("  No pitching stats aggregated.")
                else:
                    logger.info(f"  Aggregated {len(pitching_results)} pitching records.")

                    if dry_run:
                        logger.info("  [DRY-RUN] Pitching statistics that would be saved:")
                        for r in pitching_results:
                            logger.info(
                                "    Team: %-10s (%s) | G: %-3d | W-L-T: %d-%d-%d | IP: %.1f | ER: %-3d | ERA: %.2f | WHIP: %.2f",
                                r.get("team_name", r["team_id"]),
                                r["team_id"],
                                r["games"],
                                r["wins"],
                                r["losses"],
                                r["ties"],
                                r["innings_pitched"],
                                r["earned_runs"],
                                r["era"],
                                r["whip"],
                            )
                    else:
                        logger.info(f"  💾 Upserted {len(pitching_results)} team pitching rows to DB.")

            except Exception as e:
                logger.exception(f"❌ Failed pitching stats rollup for season={season}: {e}")
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
        "--type", choices=["batting", "pitching", "all"], default="all", help="Ignore this legacy argument"
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
            verbose=args.verbose,
        )
    )


if __name__ == "__main__":
    main()
