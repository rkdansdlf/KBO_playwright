import argparse
import logging

from src.aggregators.team_stat_aggregator import TeamStatAggregator
from src.db.engine import SessionLocal
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching

logger = logging.getLogger(__name__)


class TeamStatAudit:
    """
    Audit tool to compare official team season stats with calculated fallback stats.
    """

    @staticmethod
    def audit_batting(year: int, league: str = "REGULAR"):
        logger.info(f"🕵️  Auditing TEAM BATTING stats for {year} {league}...")
        with SessionLocal() as session:
            # Get official stats (source is not FALLBACK)
            official = session.query(TeamSeasonBatting).filter_by(season=year, league=league.upper()).all()
            if not official:
                logger.info("   ⚠️ No official team batting stats found.")
                return

            calculated = TeamStatAggregator.aggregate_team_batting(session, year, league)
            calc_map = {s["team_id"]: s for s in calculated}

            mismatches = 0
            for off in official:
                calc = calc_map.get(off.team_id)
                if not calc:
                    logger.info(f"   ❌ Missing calculated data for {off.team_name} ({off.team_id})")
                    mismatches += 1
                    continue

                # Check key counting stats
                keys = ["games", "at_bats", "hits", "home_runs", "runs"]
                diffs = []
                for k in keys:
                    off_val = getattr(off, k) or 0
                    calc_val = calc.get(k) or 0
                    if off_val != calc_val:
                        diffs.append(f"{k}: {off_val} vs {calc_val}")

                if diffs:
                    logger.info(f"   ❌ Mismatch [{off.team_name}]: {', '.join(diffs)}")
                    mismatches += 1

            if mismatches == 0:
                logger.info(f"   ✅ All {len(official)} team batting records match perfectly!")
            else:
                logger.info(f"   ⚠️ Found {mismatches} mismatches.")

    @staticmethod
    def audit_pitching(year: int, league: str = "REGULAR"):
        logger.info(f"🕵️  Auditing TEAM PITCHING stats for {year} {league}...")
        with SessionLocal() as session:
            official = session.query(TeamSeasonPitching).filter_by(season=year, league=league.upper()).all()
            if not official:
                logger.info("   ⚠️ No official team pitching stats found.")
                return

            calculated = TeamStatAggregator.aggregate_team_pitching(session, year, league)
            calc_map = {s["team_id"]: s for s in calculated}

            mismatches = 0
            for off in official:
                calc = calc_map.get(off.team_id)
                if not calc:
                    logger.info(f"   ❌ Missing calculated data for {off.team_name}")
                    mismatches += 1
                    continue

                # Check key counting stats
                # Note: wins/losses for team stats should match sum of pitcher stats
                keys = ["games", "wins", "losses", "earned_runs", "strikeouts"]
                diffs = []
                for k in keys:
                    off_val = getattr(off, k) or 0
                    calc_val = calc.get(k) or 0
                    if off_val != calc_val:
                        diffs.append(f"{k}: {off_val} vs {calc_val}")

                if diffs:
                    logger.info(f"   ❌ Mismatch [{off.team_name}]: {', '.join(diffs)}")
                    mismatches += 1

            if mismatches == 0:
                logger.info(f"   ✅ All {len(official)} team pitching records match perfectly!")
            else:
                logger.info(f"   ⚠️ Found {mismatches} mismatches.")


def main():
    parser = argparse.ArgumentParser(description="Audit team stats aggregation.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--league", type=str, default="regular")

    args = parser.parse_args()

    TeamStatAudit.audit_batting(args.year, args.league)
    TeamStatAudit.audit_pitching(args.year, args.league)


if __name__ == "__main__":
    main()
