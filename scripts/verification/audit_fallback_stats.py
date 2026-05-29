import argparse
import logging
import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.db.engine import SessionLocal
from src.models.player import (
    PlayerBasic,
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.repositories.player_stats_repository import PlayerSeasonBaserunningRepository, PlayerSeasonFieldingRepository
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.utils.fallback_monitor import FallbackMonitor

logger = logging.getLogger("audit_fix")


class StatAudit:
    """
    Compares officially crawled KBO stats with our fallback aggregation logic
    to identify discrepancies and optionally fix them.
    """

    @staticmethod
    def _is_safe_to_fix(off_val: int, calc_val: int, max_diff: int = 15, max_ratio: float = 0.5) -> bool:
        if off_val == 0 and calc_val > 0:
            return True  # Recovery of missing data
        diff = abs(off_val - calc_val)
        if diff <= 5:
            return True  # Minor diff
        if diff > max_diff:
            return False  # Too large
        return not (off_val > 0 and (diff / off_val) > max_ratio)

    @staticmethod
    def audit_batting(year: int, series: str, fix: bool = False):
        print(f"🕵️  Auditing BATTING stats for {year} {series} (fix={fix})...")
        with SessionLocal() as session:
            official_stats = (
                session.query(PlayerSeasonBatting)
                .filter(PlayerSeasonBatting.season == year)
                .filter(PlayerSeasonBatting.league == series.upper())
                .filter(PlayerSeasonBatting.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"]))
                .all()
            )

            if not official_stats:
                print("   ⚠️ No official batting stats found to compare.")
                return

            # Use BULK aggregation for performance
            calc_stats_list = SeasonStatAggregator.aggregate_batting_season_bulk(
                session, year, series, source="AUDIT_FIX"
            )
            calc_map = {c["player_id"]: c for c in calc_stats_list}

            mismatches = 0
            fix_count = 0
            keys_to_check = ["games", "at_bats", "hits", "home_runs", "rbi", "walks"]

            for off in official_stats:
                calc = calc_map.get(off.player_id)
                if not calc:
                    continue

                diffs = []
                for key in keys_to_check:
                    off_val = getattr(off, key) or 0
                    calc_val = calc.get(key) or 0
                    if off_val != calc_val:
                        diffs.append(f"{key}: {off_val} vs {calc_val}")

                if diffs:
                    player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
                    name = player.name if player else f"ID:{off.player_id}"
                    print(f"   ❌ Mismatch [{name} (ID:{off.player_id})]: {', '.join(diffs)}")
                    mismatches += 1

                    if fix:
                        off_games = off.games or 0
                        calc_games = calc.get("games", 0)

                        if not StatAudit._is_safe_to_fix(off_games, calc_games):
                            msg = (
                                f"🛑 Auto-fix skipped for {name}: large discrepancy ({off_games} vs {calc_games} games)"
                            )
                            print(f"      {msg}")
                            FallbackMonitor.log_fallback(year, series, "BATTING_AUDIT", msg)
                            continue

                        try:
                            # Backup
                            original_dict = {
                                col.name: getattr(off, col.name)
                                for col in off.__table__.columns
                                if not col.name.startswith("_")
                            }
                            backup_path_str = FallbackMonitor.save_audit_backup(
                                player_id=str(off.player_id),
                                type_name="batting",
                                original_data=original_dict,
                                calculated_data=calc,
                            )
                            backup_name = os.path.basename(backup_path_str)

                            calc["player_name"] = name
                            if not calc.get("team_code"):
                                calc["team_code"] = off.team_code

                            save_batting_stats_safe([calc])
                            print(f"      ✅ Fixed {name} in DB. (Backup: {backup_name})")
                            fix_count += 1
                        except Exception as e:
                            print(f"      ⚠️ Failed to fix {name}: {e}")
                            logger.error(f"Failed to fix {name} batting: {e}")

            summary_msg = f"Audited {len(official_stats)} records. Mismatches: {mismatches}, Fixed: {fix_count}"
            print(f"   📊 {summary_msg}")
            if mismatches > fix_count:
                FallbackMonitor.log_fallback(year, series, "BATTING_AUDIT", summary_msg)

    @staticmethod
    def audit_pitching(year: int, series: str, fix: bool = False):
        print(f"🕵️  Auditing PITCHING stats for {year} {series} (fix={fix})...")
        with SessionLocal() as session:
            official_stats = (
                session.query(PlayerSeasonPitching)
                .filter(PlayerSeasonPitching.season == year)
                .filter(PlayerSeasonPitching.league == series.upper())
                .filter(PlayerSeasonPitching.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"]))
                .all()
            )

            if not official_stats:
                print("   ⚠️ No official pitching stats found to compare.")
                return

            # Use BULK aggregation for performance
            calc_stats_list = SeasonStatAggregator.aggregate_pitching_season_bulk(
                session, year, series, source="AUDIT_FIX"
            )
            calc_map = {c["player_id"]: c for c in calc_stats_list}

            mismatches = 0
            fix_count = 0
            keys_to_check = ["games", "wins", "losses", "saves", "earned_runs", "innings_outs"]

            for off in official_stats:
                calc = calc_map.get(off.player_id)
                if not calc:
                    continue

                diffs = []
                for key in keys_to_check:
                    off_val = getattr(off, key) or 0
                    calc_val = calc.get(key) or 0
                    if off_val != calc_val:
                        diffs.append(f"{key}: {off_val} vs {calc_val}")

                if diffs:
                    player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
                    name = player.name if player else f"ID:{off.player_id}"
                    print(f"   ❌ Mismatch [{name} (ID:{off.player_id})]: {', '.join(diffs)}")
                    mismatches += 1

                    if fix:
                        off_games = off.games or 0
                        calc_games = calc.get("games", 0)

                        if not StatAudit._is_safe_to_fix(off_games, calc_games):
                            msg = (
                                f"🛑 Auto-fix skipped for {name}: large discrepancy ({off_games} vs {calc_games} games)"
                            )
                            print(f"      {msg}")
                            FallbackMonitor.log_fallback(year, series, "PITCHING_AUDIT", msg)
                            continue

                        try:
                            # Backup
                            original_dict = {
                                col.name: getattr(off, col.name)
                                for col in off.__table__.columns
                                if not col.name.startswith("_")
                            }
                            backup_path_str = FallbackMonitor.save_audit_backup(
                                player_id=str(off.player_id),
                                type_name="pitching",
                                original_data=original_dict,
                                calculated_data=calc,
                            )
                            backup_name = os.path.basename(backup_path_str)

                            calc["player_name"] = name
                            if not calc.get("team_code"):
                                calc["team_code"] = off.team_code

                            save_pitching_stats_to_db([calc])
                            print(f"      ✅ Fixed {name} in DB. (Backup: {backup_name})")
                            fix_count += 1
                        except Exception as e:
                            print(f"      ⚠️ Failed to fix {name}: {e}")
                            logger.error(f"Failed to fix {name} pitching: {e}")

            summary_msg = f"Audited {len(official_stats)} records. Mismatches: {mismatches}, Fixed: {fix_count}"
            print(f"   📊 {summary_msg}")
            if mismatches > fix_count:
                FallbackMonitor.log_fallback(year, series, "PITCHING_AUDIT", summary_msg)

    @staticmethod
    def audit_fielding(year: int, series: str, fix: bool = False):
        print(f"🕵️  Auditing FIELDING stats for {year} {series} (fix={fix})...")
        with SessionLocal() as session:
            official_stats = (
                session.query(PlayerSeasonFielding)
                .filter(PlayerSeasonFielding.year == year)
                .filter(PlayerSeasonFielding.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"]))
                .all()
            )

            if not official_stats:
                print("   ⚠️ No official fielding stats found to compare.")
                return

            mismatches = 0
            fix_count = 0
            repo = PlayerSeasonFieldingRepository()

            # Group official by player to reduce redundant aggregations
            players = sorted(list({off.player_id for off in official_stats}))

            for pid in players:
                calc_list = SeasonStatAggregator.aggregate_fielding_season(
                    session, pid, year, series, source="AUDIT_FIX"
                )
                if not calc_list:
                    continue

                # Check each position for this player
                for off in [o for o in official_stats if o.player_id == pid]:
                    calc = next((c for c in calc_list if c["position_id"] == off.position_id), None)
                    if not calc:
                        continue

                    if (off.errors or 0) != (calc.get("errors") or 0):
                        player = session.query(PlayerBasic).filter_by(player_id=pid).first()
                        name = player.name if player else f"ID:{pid}"
                        print(
                            f"   ❌ Mismatch [{name} - {off.position_id}]: errors {off.errors} vs {calc.get('errors')}"
                        )
                        mismatches += 1

                        if fix:
                            try:
                                # Backup
                                original_dict = {
                                    col.name: getattr(off, col.name)
                                    for col in off.__table__.columns
                                    if not col.name.startswith("_")
                                }
                                backup_path_str = FallbackMonitor.save_audit_backup(
                                    player_id=str(pid),
                                    type_name="fielding",
                                    original_data=original_dict,
                                    calculated_data=calc,
                                )
                                backup_name = os.path.basename(backup_path_str)

                                calc["team_id"] = off.team_id
                                repo.upsert_many([calc])
                                print(f"      ✅ Fixed {name} ({off.position_id}) in DB. (Backup: {backup_name})")
                                fix_count += 1
                            except Exception as e:
                                print(f"      ⚠️ Failed to fix {name}: {e}")
                                logger.error(f"Failed to fix {name} fielding: {e}")

            print(f"   Done. Mismatches: {mismatches}, Fixed: {fix_count}")

    @staticmethod
    def audit_baserunning(year: int, series: str, fix: bool = False):
        print(f"🕵️  Auditing BASERUNNING stats for {year} {series} (fix={fix})...")
        with SessionLocal() as session:
            official_stats = (
                session.query(PlayerSeasonBaserunning)
                .filter(PlayerSeasonBaserunning.year == year)
                .filter(
                    PlayerSeasonBaserunning.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"])
                )
                .all()
            )

            if not official_stats:
                print("   ⚠️ No official baserunning stats found to compare.")
                return

            mismatches = 0
            fix_count = 0
            repo = PlayerSeasonBaserunningRepository()

            for off in official_stats:
                calc = SeasonStatAggregator.aggregate_baserunning_season(
                    session, off.player_id, year, series, source="AUDIT_FIX"
                )
                if not calc:
                    continue

                keys = ["stolen_bases", "caught_stealing"]
                diffs = []
                for k in keys:
                    if (getattr(off, k) or 0) != (calc.get(k) or 0):
                        diffs.append(f"{k}: {getattr(off, k)} vs {calc.get(k)}")

                if diffs:
                    player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
                    name = player.name if player else f"ID:{off.player_id}"
                    print(f"   ❌ Mismatch [{name} (ID:{off.player_id})]: {', '.join(diffs)}")
                    mismatches += 1

                    if fix:
                        try:
                            # Backup
                            original_dict = {
                                col.name: getattr(off, col.name)
                                for col in off.__table__.columns
                                if not col.name.startswith("_")
                            }
                            backup_path_str = FallbackMonitor.save_audit_backup(
                                player_id=str(off.player_id),
                                type_name="baserunning",
                                original_data=original_dict,
                                calculated_data=calc,
                            )
                            backup_name = os.path.basename(backup_path_str)

                            calc["team_id"] = off.team_id
                            repo.upsert_many([calc])
                            print(f"      ✅ Fixed {name} in DB. (Backup: {backup_name})")
                            fix_count += 1
                        except Exception as e:
                            print(f"      ⚠️ Failed to fix {name}: {e}")
                            logger.error(f"Failed to fix {name} baserunning: {e}")

            print(f"   Done. Mismatches: {mismatches}, Fixed: {fix_count}")


def main():
    parser = argparse.ArgumentParser(description="Audit fallback aggregation accuracy.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--series", type=str, default="regular")
    parser.add_argument(
        "--type", type=str, default="all", choices=["batting", "pitching", "fielding", "baserunning", "all"]
    )
    parser.add_argument("--fix", action="store_true", help="Automatically fix mismatches in DB")

    args = parser.parse_args()

    if args.type in ["batting", "all"]:
        StatAudit.audit_batting(args.year, args.series, fix=args.fix)
    if args.type in ["pitching", "all"]:
        StatAudit.audit_pitching(args.year, args.series, fix=args.fix)
    if args.type in ["fielding", "all"]:
        StatAudit.audit_fielding(args.year, args.series, fix=args.fix)
    if args.type in ["baserunning", "all"]:
        StatAudit.audit_baserunning(args.year, args.series, fix=args.fix)


if __name__ == "__main__":
    main()
