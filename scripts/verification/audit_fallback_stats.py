import argparse
import math
from typing import Dict, Any, List
from src.db.engine import SessionLocal
from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching, PlayerBasic

class StatAudit:
    """
    Compares officially crawled KBO stats with our fallback aggregation logic
    to identify discrepancies.
    """

    @staticmethod
    def audit_batting(year: int, series: str):
        print(f"🕵️  Auditing BATTING stats for {year} {series}...")
        with SessionLocal() as session:
            # 1. Get official stats (source=CRAWLER or source=ROLLUP but not FALLBACK)
            official_stats = (
                session.query(PlayerSeasonBatting)
                .filter(PlayerSeasonBatting.season == year)
                .filter(PlayerSeasonBatting.league == series.upper())
                .filter(PlayerSeasonBatting.source != 'FALLBACK')
                .all()
            )
            
            if not official_stats:
                print("   ⚠️ No official batting stats found to compare.")
                return

            mismatches = 0
            for off in official_stats:
                # 2. Generate fallback aggregate
                calc = SeasonStatAggregator.aggregate_batting_season(session, off.player_id, year, series)
                if not calc:
                    continue
                
                # 3. Compare key counting stats
                keys_to_check = ['games', 'at_bats', 'hits', 'home_runs', 'rbi', 'walks']
                diffs = []
                for key in keys_to_check:
                    off_val = getattr(off, key) or 0
                    calc_val = calc.get(key) or 0
                    if off_val != calc_val:
                        diffs.append(f"{key}: {off_val} vs {calc_val}")
                
                if diffs:
                    player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
                    name = player.name if player else f"ID:{off.player_id}"
                    print(f"   ❌ Mismatch [{name}]: {', '.join(diffs)}")
                    mismatches += 1
            
            if mismatches == 0:
                print(f"   ✅ All {len(official_stats)} batting records match!")
            else:
                print(f"   ⚠️ Found {mismatches} mismatches out of {len(official_stats)}.")

    @staticmethod
    def audit_pitching(year: int, series: str):
        print(f"🕵️  Auditing PITCHING stats for {year} {series}...")
        with SessionLocal() as session:
            official_stats = (
                session.query(PlayerSeasonPitching)
                .filter(PlayerSeasonPitching.season == year)
                .filter(PlayerSeasonPitching.league == series.upper())
                .filter(PlayerSeasonPitching.source != 'FALLBACK')
                .all()
            )
            
            if not official_stats:
                print("   ⚠️ No official pitching stats found to compare.")
                return

            mismatches = 0
            for off in official_stats:
                calc = SeasonStatAggregator.aggregate_pitching_season(session, off.player_id, year, series)
                if not calc:
                    continue
                
                # Compare key stats
                # Note: innings_outs is the absolute truth, innings_pitched might have float diffs
                keys_to_check = ['games', 'wins', 'losses', 'saves', 'earned_runs', 'innings_outs']
                diffs = []
                for key in keys_to_check:
                    off_val = getattr(off, key) or 0
                    calc_val = calc.get(key) or 0
                    if off_val != calc_val:
                        diffs.append(f"{key}: {off_val} vs {calc_val}")
                
                if diffs:
                    player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
                    name = player.name if player else f"ID:{off.player_id}"
                    print(f"   ❌ Mismatch [{name}]: {', '.join(diffs)}")
                    mismatches += 1
            
            if mismatches == 0:
                print(f"   ✅ All {len(official_stats)} pitching records match!")
            else:
                print(f"   ⚠️ Found {mismatches} mismatches out of {len(official_stats)}.")

def main():
    parser = argparse.ArgumentParser(description="Audit fallback aggregation accuracy.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--series", type=str, default="regular")
    parser.add_argument("--type", type=str, default="all", choices=["batting", "pitching", "all"])
    
    args = parser.parse_args()
    
    if args.type in ["batting", "all"]:
        StatAudit.audit_batting(args.year, args.series)
    if args.type in ["pitching", "all"]:
        StatAudit.audit_pitching(args.year, args.series)

if __name__ == "__main__":
    main()
