import argparse
import math
import logging
from typing import Dict, Any, List
from src.db.engine import SessionLocal
from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching, PlayerBasic
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.utils.fallback_monitor import FallbackMonitor

logger = logging.getLogger("audit_fix")

class StatAudit:
    """
    Compares officially crawled KBO stats with our fallback aggregation logic
    to identify discrepancies and optionally fix them.
    """

    @staticmethod
    def audit_batting(year: int, series: str, fix: bool = False):
        print(f"🕵️  Auditing BATTING stats for {year} {series} (fix={fix})...")
        with SessionLocal() as session:
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
            fix_count = 0
            for off in official_stats:
                calc = SeasonStatAggregator.aggregate_batting_season(session, off.player_id, year, series)
                if not calc:
                    continue
                
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

                    if fix:
                        # Safety Guard: If game diff > 10, skip auto-fix
                        off_games = off.games or 0
                        calc_games = calc.get('games', 0)
                        if abs(off_games - calc_games) > 10:
                            msg = f"🛑 Auto-fix skipped for {name}: large discrepancy ({off_games} vs {calc_games} games)"
                            print(f"      {msg}")
                            FallbackMonitor.log_fallback(year, series, "BATTING_AUDIT", msg)
                            continue

                        try:
                            # Repositories expect a list of dicts
                            # For batting, we need to ensure player_name/team_code are included if repo requires them
                            calc['player_name'] = name
                            # Resolve team code from original if missing
                            if not calc.get('team_code'):
                                calc['team_code'] = off.team_code
                            
                            save_batting_stats_safe([calc])
                            print(f"      ✅ Fixed {name} in DB.")
                            fix_count += 1
                        except Exception as e:
                            print(f"      ⚠️ Failed to fix {name}: {e}")
            
            if mismatches == 0:
                print(f"   ✅ All {len(official_stats)} batting records match!")
            else:
                msg = f"Found {mismatches} mismatches out of {len(official_stats)}."
                if fix:
                    msg += f" Successfully fixed {fix_count} records."
                print(f"   ⚠️ {msg}")
                if not fix or (mismatches > fix_count):
                    FallbackMonitor.log_fallback(year, series, "BATTING_AUDIT", msg)

    @staticmethod
    def audit_pitching(year: int, series: str, fix: bool = False):
        print(f"🕵️  Auditing PITCHING stats for {year} {series} (fix={fix})...")
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
            fix_count = 0
            for off in official_stats:
                calc = SeasonStatAggregator.aggregate_pitching_season(session, off.player_id, year, series)
                if not calc:
                    continue
                
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

                    if fix:
                        # Safety Guard
                        off_games = off.games or 0
                        calc_games = calc.get('games', 0)
                        if abs(off_games - calc_games) > 10:
                            msg = f"🛑 Auto-fix skipped for {name}: large discrepancy ({off_games} vs {calc_games} games)"
                            print(f"      {msg}")
                            FallbackMonitor.log_fallback(year, series, "PITCHING_AUDIT", msg)
                            continue

                        try:
                            # Pitching repo expects payloads from stats list to_repository_payload
                            # We'll simulate that structure or ensure it matches
                            calc['player_name'] = name
                            if not calc.get('team_code'):
                                calc['team_code'] = off.team_code
                            
                            save_pitching_stats_to_db([calc])
                            print(f"      ✅ Fixed {name} in DB.")
                            fix_count += 1
                        except Exception as e:
                            print(f"      ⚠️ Failed to fix {name}: {e}")
            
            if mismatches == 0:
                print(f"   ✅ All {len(official_stats)} pitching records match!")
            else:
                msg = f"Found {mismatches} mismatches out of {len(official_stats)}."
                if fix:
                    msg += f" Successfully fixed {fix_count} records."
                print(f"   ⚠️ {msg}")
                if not fix or (mismatches > fix_count):
                    FallbackMonitor.log_fallback(year, series, "PITCHING_AUDIT", msg)

def main():
    parser = argparse.ArgumentParser(description="Audit fallback aggregation accuracy.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--series", type=str, default="regular")
    parser.add_argument("--type", type=str, default="all", choices=["batting", "pitching", "all"])
    parser.add_argument("--fix", action="store_true", help="Automatically fix mismatches in DB")
    
    args = parser.parse_args()
    
    if args.type in ["batting", "all"]:
        StatAudit.audit_batting(args.year, args.series, fix=args.fix)
    if args.type in ["pitching", "all"]:
        StatAudit.audit_pitching(args.year, args.series, fix=args.fix)

if __name__ == "__main__":
    main()
