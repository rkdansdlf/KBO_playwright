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
from src.utils.alerting import SlackWebhookClient
from src.utils.fallback_monitor import FallbackMonitor

logger = logging.getLogger("audit_fix")


class StatAudit:
    """
    Compares officially crawled KBO stats with our fallback aggregation logic
    to identify discrepancies and optionally fix them if they pass safety checks.
    """

    @staticmethod
    def send_remediation_abort_alert(year: int, series: str, category: str, reason: str):
        msg = f"🛑 *KBO Auto-Remediation Aborted ({category})*"
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "🛑 Auto-Remediation Aborted"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Year:* {year}"},
                    {"type": "mrkdwn", "text": f"*Series:* {series}"},
                    {"type": "mrkdwn", "text": f"*Category:* {category}"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Reason for Aborting:*\n{reason}"}},
        ]
        SlackWebhookClient.send_alert(msg, blocks=blocks)
        try:
            FallbackMonitor.save_audit_event(category, "abort", {"year": year, "series": series, "reason": reason})
        except Exception as e:
            logger.error(f"Failed to save audit abort event: {e}")

    @staticmethod
    def send_remediation_success_alert(
        year: int, series: str, category: str, mismatches_count: int, fixed_players: list[dict]
    ):
        """Send Telegram/Slack alert when auto-remediation successfully fixes mismatches."""
        top_players = fixed_players[:5]
        player_lines = ""
        for p in top_players:
            diffs_str = ", ".join(p.get("diffs", [])[:3])
            player_lines += f"  • {p['name']}: {diffs_str}\n"
        if len(fixed_players) > 5:
            player_lines += f"  … 외 {len(fixed_players) - 5}명\n"

        msg = (
            f"<b>✅ KBO Auto-Remediation 완료 ({category})</b>\n"
            f"연도: {year} | 시리즈: {series}\n"
            f"수정: {len(fixed_players)}/{mismatches_count}건\n\n"
            f"{player_lines}"
        )
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"✅ Auto-Remediation 완료 ({category})"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Year:* {year}"},
                    {"type": "mrkdwn", "text": f"*Series:* {series}"},
                    {"type": "mrkdwn", "text": f"*수정:* {len(fixed_players)}/{mismatches_count}건"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*수정된 선수:*\n{player_lines or '(없음)'}"}},
        ]
        SlackWebhookClient.send_alert(msg, blocks=blocks)

    @staticmethod
    def send_audit_warning_alert(year: int, series: str, category: str, mismatches: list[dict]):
        """Send warning alert when mismatches are detected but --fix is disabled."""
        top = mismatches[:5]
        player_lines = ""
        for m in top:
            diffs_str = ", ".join(m.get("diffs", [])[:3])
            player_lines += f"  • {m['name']}: {diffs_str}\n"
        if len(mismatches) > 5:
            player_lines += f"  … 외 {len(mismatches) - 5}명\n"

        msg = (
            f"<b>⚠️ KBO Stats Mismatch 발견 ({category}) — 자동 수정 비활성화</b>\n"
            f"연도: {year} | 시리즈: {series}\n"
            f"불일치: {len(mismatches)}건\n\n"
            f"{player_lines}"
            "DAILY_AUTO_REMEDIATION=1 또는 --fix 플래그로 자동 수정을 활성화하세요."
        )
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"⚠️ Stats Mismatch 감지 ({category})"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Year:* {year}"},
                    {"type": "mrkdwn", "text": f"*Series:* {series}"},
                    {"type": "mrkdwn", "text": f"*불일치:* {len(mismatches)}건"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*불일치 선수:*\n{player_lines or '(없음)'}"}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "_자동 수정 비활성화 중. `--fix` 플래그 또는 `DAILY_AUTO_REMEDIATION=1` 설정으로 활성화하세요._",
                },
            },
        ]
        SlackWebhookClient.send_alert(msg, blocks=blocks)
        try:
            mismatch_data = []
            for m in mismatches:
                mismatch_data.append(
                    {
                        "player_id": str(m["player_id"]),
                        "name": str(m["name"]),
                        "diffs": m["diffs"],
                    }
                )
            FallbackMonitor.save_audit_event(
                category, "warning", {"year": year, "series": series, "mismatches": mismatch_data}
            )
        except Exception as e:
            logger.error(f"Failed to save audit warning event: {e}")

    @staticmethod
    def audit_batting(year: int, series: str, fix: bool = False, max_mismatches: int = 10, max_game_diff: int = 15):
        print(
            f"🕵️  Auditing BATTING stats for {year} {series} (fix={fix}, max_mismatches={max_mismatches}, max_game_diff={max_game_diff})..."
        )
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

            mismatches = []
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
                    mismatches.append(
                        {
                            "player_id": off.player_id,
                            "name": name,
                            "diffs": diffs,
                            "off_record": off,
                            "calc_data": calc,
                            "off_games": off.games or 0,
                            "calc_games": calc.get("games", 0),
                        }
                    )

            mismatches_count = len(mismatches)
            if mismatches_count == 0:
                print("   ✅ No batting mismatches found.")
                return

            print(f"   ❌ Found {mismatches_count} batting mismatches.")
            for m in mismatches:
                print(f"      - {m['name']} (ID:{m['player_id']}): {', '.join(m['diffs'])}")

            if not fix:
                print("   ℹ️ Fix is disabled. Mismatches not resolved in DB.")
                StatAudit.send_audit_warning_alert(year, series, "BATTING", mismatches)
                return

            # Check safety thresholds
            abort_remediation = False
            abort_reasons = []

            if mismatches_count > max_mismatches:
                abort_remediation = True
                abort_reasons.append(f"Total mismatches ({mismatches_count}) exceeds threshold of {max_mismatches}")

            for m in mismatches:
                diff_games = abs(m["off_games"] - m["calc_games"])
                if diff_games > max_game_diff:
                    abort_remediation = True
                    abort_reasons.append(
                        f"Player {m['name']} (ID:{m['player_id']}) has game difference of {diff_games} "
                        f"(Official: {m['off_games']}, Calculated: {m['calc_games']}), which exceeds threshold of {max_game_diff}"
                    )

            if abort_remediation:
                reason_str = "; ".join(abort_reasons)
                msg = (
                    f"🛑 Auto-remediation ABORTED for BATTING ({year} {series}) due to safety violations: {reason_str}"
                )
                print(f"   {msg}")
                logger.error(msg)
                StatAudit.send_remediation_abort_alert(year, series, "BATTING", reason_str)
                return

            fix_count = 0
            for m in mismatches:
                off = m["off_record"]
                calc = m["calc_data"]
                name = m["name"]
                try:
                    # Backup original data
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
                        player_name=name,
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

            summary_msg = f"Audited {len(official_stats)} records. Mismatches: {mismatches_count}, Fixed: {fix_count}"
            print(f"   📊 {summary_msg}")
            if fix_count > 0:
                fixed_players = [m for m in mismatches if m["name"] is not None][:fix_count]
                StatAudit.send_remediation_success_alert(year, series, "BATTING", mismatches_count, fixed_players)
            if mismatches_count > fix_count:
                FallbackMonitor.log_fallback(year, series, "BATTING_AUDIT", summary_msg)

    @staticmethod
    def audit_pitching(
        year: int,
        series: str,
        fix: bool = False,
        max_mismatches: int = 10,
        max_game_diff: int = 15,
        max_innings_outs_diff: int = 45,
    ):
        print(
            f"🕵️  Auditing PITCHING stats for {year} {series} (fix={fix}, max_mismatches={max_mismatches}, max_game_diff={max_game_diff}, max_innings_outs_diff={max_innings_outs_diff})..."
        )
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

            mismatches = []
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
                    mismatches.append(
                        {
                            "player_id": off.player_id,
                            "name": name,
                            "diffs": diffs,
                            "off_record": off,
                            "calc_data": calc,
                            "off_games": off.games or 0,
                            "calc_games": calc.get("games", 0),
                            "off_outs": off.innings_outs or 0,
                            "calc_outs": calc.get("innings_outs", 0),
                        }
                    )

            mismatches_count = len(mismatches)
            if mismatches_count == 0:
                print("   ✅ No pitching mismatches found.")
                return

            print(f"   ❌ Found {mismatches_count} pitching mismatches.")
            for m in mismatches:
                print(f"      - {m['name']} (ID:{m['player_id']}): {', '.join(m['diffs'])}")

            if not fix:
                print("   ℹ️ Fix is disabled. Mismatches not resolved in DB.")
                StatAudit.send_audit_warning_alert(year, series, "PITCHING", mismatches)
                return

            # Check safety thresholds
            abort_remediation = False
            abort_reasons = []

            if mismatches_count > max_mismatches:
                abort_remediation = True
                abort_reasons.append(f"Total mismatches ({mismatches_count}) exceeds threshold of {max_mismatches}")

            for m in mismatches:
                diff_games = abs(m["off_games"] - m["calc_games"])
                if diff_games > max_game_diff:
                    abort_remediation = True
                    abort_reasons.append(
                        f"Player {m['name']} (ID:{m['player_id']}) has game difference of {diff_games} "
                        f"(Official: {m['off_games']}, Calculated: {m['calc_games']}), which exceeds threshold of {max_game_diff}"
                    )
                diff_outs = abs(m["off_outs"] - m["calc_outs"])
                if diff_outs > max_innings_outs_diff:
                    abort_remediation = True
                    abort_reasons.append(
                        f"Player {m['name']} (ID:{m['player_id']}) has innings outs difference of {diff_outs} "
                        f"(Official: {m['off_outs']}, Calculated: {m['calc_outs']}), which exceeds threshold of {max_innings_outs_diff}"
                    )

            if abort_remediation:
                reason_str = "; ".join(abort_reasons)
                msg = (
                    f"🛑 Auto-remediation ABORTED for PITCHING ({year} {series}) due to safety violations: {reason_str}"
                )
                print(f"   {msg}")
                logger.error(msg)
                StatAudit.send_remediation_abort_alert(year, series, "PITCHING", reason_str)
                return

            fix_count = 0
            for m in mismatches:
                off = m["off_record"]
                calc = m["calc_data"]
                name = m["name"]
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
                        player_name=name,
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

            summary_msg = f"Audited {len(official_stats)} records. Mismatches: {mismatches_count}, Fixed: {fix_count}"
            print(f"   📊 {summary_msg}")
            if fix_count > 0:
                fixed_players = [m for m in mismatches if m["name"] is not None][:fix_count]
                StatAudit.send_remediation_success_alert(year, series, "PITCHING", mismatches_count, fixed_players)
            if mismatches_count > fix_count:
                FallbackMonitor.log_fallback(year, series, "PITCHING_AUDIT", summary_msg)

    @staticmethod
    def audit_fielding(year: int, series: str, fix: bool = False, max_mismatches: int = 15, max_error_diff: int = 5):
        print(
            f"🕵️  Auditing FIELDING stats for {year} {series} (fix={fix}, max_mismatches={max_mismatches}, max_error_diff={max_error_diff})..."
        )
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

            mismatches = []
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

                    off_errors = off.errors or 0
                    calc_errors = calc.get("errors") or 0
                    if off_errors != calc_errors:
                        player = session.query(PlayerBasic).filter_by(player_id=pid).first()
                        name = player.name if player else f"ID:{pid}"
                        mismatches.append(
                            {
                                "player_id": pid,
                                "name": name,
                                "position_id": off.position_id,
                                "off_record": off,
                                "calc_data": calc,
                                "off_errors": off_errors,
                                "calc_errors": calc_errors,
                            }
                        )

            mismatches_count = len(mismatches)
            if mismatches_count == 0:
                print("   ✅ No fielding mismatches found.")
                return

            print(f"   ❌ Found {mismatches_count} fielding mismatches.")
            for m in mismatches:
                print(f"      - {m['name']} - {m['position_id']}: errors {m['off_errors']} vs {m['calc_errors']}")

            if not fix:
                print("   ℹ️ Fix is disabled. Mismatches not resolved in DB.")
                StatAudit.send_audit_warning_alert(year, series, "FIELDING", mismatches)
                return

            # Check safety thresholds
            abort_remediation = False
            abort_reasons = []

            if mismatches_count > max_mismatches:
                abort_remediation = True
                abort_reasons.append(f"Total mismatches ({mismatches_count}) exceeds threshold of {max_mismatches}")

            for m in mismatches:
                diff_errors = abs(m["off_errors"] - m["calc_errors"])
                if diff_errors > max_error_diff:
                    abort_remediation = True
                    abort_reasons.append(
                        f"Player {m['name']} (ID:{m['player_id']}) has error difference of {diff_errors} for position {m['position_id']} "
                        f"(Official: {m['off_errors']}, Calculated: {m['calc_errors']}), which exceeds threshold of {max_error_diff}"
                    )

            if abort_remediation:
                reason_str = "; ".join(abort_reasons)
                msg = (
                    f"🛑 Auto-remediation ABORTED for FIELDING ({year} {series}) due to safety violations: {reason_str}"
                )
                print(f"   {msg}")
                logger.error(msg)
                StatAudit.send_remediation_abort_alert(year, series, "FIELDING", reason_str)
                return

            fix_count = 0
            for m in mismatches:
                off = m["off_record"]
                calc = m["calc_data"]
                name = m["name"]
                pid = m["player_id"]
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
                        player_name=name,
                    )
                    backup_name = os.path.basename(backup_path_str)

                    calc["team_id"] = off.team_id
                    repo.upsert_many([calc])
                    print(f"      ✅ Fixed {name} ({off.position_id}) in DB. (Backup: {backup_name})")
                    fix_count += 1
                except Exception as e:
                    print(f"      ⚠️ Failed to fix {name}: {e}")
                    logger.error(f"Failed to fix {name} fielding: {e}")

            print(f"   Done. Mismatches: {mismatches_count}, Fixed: {fix_count}")
            if fix_count > 0:
                fixed_players = [m for m in mismatches if m["name"] is not None][:fix_count]
                StatAudit.send_remediation_success_alert(year, series, "FIELDING", mismatches_count, fixed_players)

    @staticmethod
    def audit_baserunning(
        year: int, series: str, fix: bool = False, max_mismatches: int = 15, max_sb_cs_diff: int = 10
    ):
        print(
            f"🕵️  Auditing BASERUNNING stats for {year} {series} (fix={fix}, max_mismatches={max_mismatches}, max_sb_cs_diff={max_sb_cs_diff})..."
        )
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

            mismatches = []
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
                    off_val = getattr(off, k) or 0
                    calc_val = calc.get(k) or 0
                    if off_val != calc_val:
                        diffs.append(f"{k}: {off_val} vs {calc_val}")

                if diffs:
                    player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
                    name = player.name if player else f"ID:{off.player_id}"
                    mismatches.append(
                        {
                            "player_id": off.player_id,
                            "name": name,
                            "diffs": diffs,
                            "off_record": off,
                            "calc_data": calc,
                            "off_sb": off.stolen_bases or 0,
                            "calc_sb": calc.get("stolen_bases", 0),
                            "off_cs": off.caught_stealing or 0,
                            "calc_cs": calc.get("caught_stealing", 0),
                        }
                    )

            mismatches_count = len(mismatches)
            if mismatches_count == 0:
                print("   ✅ No baserunning mismatches found.")
                return

            print(f"   ❌ Found {mismatches_count} baserunning mismatches.")
            for m in mismatches:
                print(f"      - {m['name']} (ID:{m['player_id']}): {', '.join(m['diffs'])}")

            if not fix:
                print("   ℹ️ Fix is disabled. Mismatches not resolved in DB.")
                StatAudit.send_audit_warning_alert(year, series, "BASERUNNING", mismatches)
                return

            # Check safety thresholds
            abort_remediation = False
            abort_reasons = []

            if mismatches_count > max_mismatches:
                abort_remediation = True
                abort_reasons.append(f"Total mismatches ({mismatches_count}) exceeds threshold of {max_mismatches}")

            for m in mismatches:
                diff_sb = abs(m["off_sb"] - m["calc_sb"])
                diff_cs = abs(m["off_cs"] - m["calc_cs"])
                if diff_sb > max_sb_cs_diff:
                    abort_remediation = True
                    abort_reasons.append(
                        f"Player {m['name']} (ID:{m['player_id']}) has stolen bases difference of {diff_sb} "
                        f"(Official: {m['off_sb']}, Calculated: {m['calc_sb']}), which exceeds threshold of {max_sb_cs_diff}"
                    )
                if diff_cs > max_sb_cs_diff:
                    abort_remediation = True
                    abort_reasons.append(
                        f"Player {m['name']} (ID:{m['player_id']}) has caught stealing difference of {diff_cs} "
                        f"(Official: {m['off_cs']}, Calculated: {m['calc_cs']}), which exceeds threshold of {max_sb_cs_diff}"
                    )

            if abort_remediation:
                reason_str = "; ".join(abort_reasons)
                msg = f"🛑 Auto-remediation ABORTED for BASERUNNING ({year} {series}) due to safety violations: {reason_str}"
                print(f"   {msg}")
                logger.error(msg)
                StatAudit.send_remediation_abort_alert(year, series, "BASERUNNING", reason_str)
                return

            fix_count = 0
            for m in mismatches:
                off = m["off_record"]
                calc = m["calc_data"]
                name = m["name"]
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
                        player_name=name,
                    )
                    backup_name = os.path.basename(backup_path_str)

                    calc["team_id"] = off.team_id
                    repo.upsert_many([calc])
                    print(f"      ✅ Fixed {name} in DB. (Backup: {backup_name})")
                    fix_count += 1
                except Exception as e:
                    print(f"      ⚠️ Failed to fix {name}: {e}")
                    logger.error(f"Failed to fix {name} baserunning: {e}")

            print(f"   Done. Mismatches: {mismatches_count}, Fixed: {fix_count}")
            if fix_count > 0:
                fixed_players = [m for m in mismatches if m["name"] is not None][:fix_count]
                StatAudit.send_remediation_success_alert(year, series, "BASERUNNING", mismatches_count, fixed_players)


def main():
    parser = argparse.ArgumentParser(description="Audit fallback aggregation accuracy.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--series", type=str, default="regular")
    parser.add_argument(
        "--type", type=str, default="all", choices=["batting", "pitching", "fielding", "baserunning", "all"]
    )
    parser.add_argument("--fix", action="store_true", help="Automatically fix mismatches in DB")
    parser.add_argument(
        "--max-mismatches", type=int, default=10, help="Max mismatches allowed before aborting auto-fix"
    )
    parser.add_argument(
        "--max-game-diff",
        type=int,
        default=15,
        help="Max game difference allowed for a player before aborting auto-fix",
    )
    parser.add_argument(
        "--max-innings-outs-diff",
        type=int,
        default=45,
        help="Max innings outs difference allowed for a pitcher before aborting auto-fix",
    )
    parser.add_argument(
        "--max-error-diff",
        type=int,
        default=5,
        help="Max error difference allowed for a fielder before aborting auto-fix",
    )
    parser.add_argument(
        "--max-sb-cs-diff",
        type=int,
        default=10,
        help="Max stolen bases / caught stealing difference allowed for a runner before aborting auto-fix",
    )

    args = parser.parse_args()

    # Enable auto-remediation if --fix is passed OR DAILY_AUTO_REMEDIATION=1 is set in env
    fix_enabled = args.fix or os.getenv("DAILY_AUTO_REMEDIATION", "0") == "1"

    # Dynamic defaults for max_mismatches depending on if type is 'all' or custom
    max_mismatches_fielding = args.max_mismatches if "--max-mismatches" in sys.argv else 15
    max_mismatches_baserunning = args.max_mismatches if "--max-mismatches" in sys.argv else 15

    if args.type in ["batting", "all"]:
        StatAudit.audit_batting(
            args.year,
            args.series,
            fix=fix_enabled,
            max_mismatches=args.max_mismatches,
            max_game_diff=args.max_game_diff,
        )
    if args.type in ["pitching", "all"]:
        StatAudit.audit_pitching(
            args.year,
            args.series,
            fix=fix_enabled,
            max_mismatches=args.max_mismatches,
            max_game_diff=args.max_game_diff,
            max_innings_outs_diff=args.max_innings_outs_diff,
        )
    if args.type in ["fielding", "all"]:
        StatAudit.audit_fielding(
            args.year,
            args.series,
            fix=fix_enabled,
            max_mismatches=max_mismatches_fielding,
            max_error_diff=args.max_error_diff,
        )
    if args.type in ["baserunning", "all"]:
        StatAudit.audit_baserunning(
            args.year,
            args.series,
            fix=fix_enabled,
            max_mismatches=max_mismatches_baserunning,
            max_sb_cs_diff=args.max_sb_cs_diff,
        )


if __name__ == "__main__":
    main()
