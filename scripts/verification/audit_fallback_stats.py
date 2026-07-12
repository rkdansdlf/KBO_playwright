import argparse
import logging
import os
import sys
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

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
from src.repositories.player_stats_repository import (
    PlayerSeasonBaserunningRepository,
    PlayerSeasonFieldingRepository,
)
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.utils.alerting import SlackWebhookClient
from src.utils.fallback_monitor import FallbackMonitor

logger = logging.getLogger("audit_fix")
AUDIT_EXCEPTIONS = (SQLAlchemyError, OSError, RuntimeError, ValueError, TypeError)


class StatAudit:
    """Compares officially crawled KBO stats with our fallback aggregation logic
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
        except AUDIT_EXCEPTIONS:
            logger.exception("Failed to save audit abort event")

    @staticmethod
    def send_remediation_success_alert(
        year: int,
        series: str,
        category: str,
        mismatches_count: int,
        fixed_players: list[dict],
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
            mismatch_data = [
                {
                    "player_id": str(m["player_id"]),
                    "name": str(m["name"]),
                    "diffs": m["diffs"],
                }
                for m in mismatches
            ]
            FallbackMonitor.save_audit_event(
                category,
                "warning",
                {"year": year, "series": series, "mismatches": mismatch_data},
            )
        except AUDIT_EXCEPTIONS:
            logger.exception("Failed to save audit warning event")

    @staticmethod
    def _collect_mismatches(
        official_stats: list,
        calc_map_or_fn,
        keys_to_check: list[str],
        extra_fields: dict[str, tuple[str, str]],
        session,
        is_fielding: bool = False,
    ) -> list[dict]:
        mismatches: list[dict] = []
        for off in official_stats:
            calc = (
                calc_map_or_fn.get(off.player_id)
                if not is_fielding
                else next(
                    (c for c in calc_map_or_fn.get(off.player_id, []) if c.get("position_id") == off.position_id),
                    None,
                )
            )
            if not calc:
                continue
            diffs = []
            for key in keys_to_check:
                off_val = getattr(off, key) if not is_fielding else (off.errors or 0)
                calc_val = calc.get(key) if not is_fielding else (calc.get("errors") or 0)
                if off_val != calc_val if not is_fielding else (key == "errors" and off_val != calc_val):
                    if not is_fielding or key == "errors":
                        off_val_actual = getattr(off, key) or 0
                        calc_val_actual = calc.get(key) or 0
                        if off_val_actual != calc_val_actual:
                            diffs.append(f"{key}: {off_val_actual} vs {calc_val_actual}")
            if not diffs:
                continue
            player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
            name = player.name if player else f"ID:{off.player_id}"
            entry = {"player_id": off.player_id, "name": name, "diffs": diffs, "off_record": off, "calc_data": calc}
            for field, (off_attr, _calc_key) in extra_fields.items():
                entry[field] = getattr(off, off_attr) or 0
            if is_fielding:
                entry["position_id"] = off.position_id
                entry["off_errors"] = off.errors or 0
                entry["calc_errors"] = calc.get("errors") or 0
                entry["diffs"] = [f"errors {entry['off_errors']} vs {entry['calc_errors']}"]
            mismatches.append(entry)
        return mismatches

    @staticmethod
    def _safety_check(
        mismatches: list[dict],
        max_mismatches: int,
        threshold_rules: list[tuple[str, str, int, str]],
    ) -> tuple[bool, list[str]]:
        abort_reasons = []
        if len(mismatches) > max_mismatches:
            abort_reasons.append(f"Total mismatches ({len(mismatches)}) exceeds threshold of {max_mismatches}")
        for m in mismatches:
            for field, _label, threshold, template in threshold_rules:
                diff = abs(m.get(field, 0) - m.get(field.replace("off_", "calc_"), 0))
                if diff > threshold:
                    abort_reasons.append(
                        template.format(
                            name=m["name"],
                            pid=m["player_id"],
                            diff=diff,
                            off=m[field],
                            calc=m.get(field.replace("off_", "calc_"), 0),
                            threshold=threshold,
                        ),
                    )
        return bool(abort_reasons), abort_reasons

    @staticmethod
    def _fix_mismatches(
        mismatches: list[dict],
        type_name: str,
        session,
        save_fn,
        extra_fields_fn=None,
    ) -> int:
        fix_count = 0
        for m in mismatches:
            off = m["off_record"]
            calc = m["calc_data"]
            name = m["name"]
            try:
                original_dict = {
                    col.name: getattr(off, col.name) for col in off.__table__.columns if not col.name.startswith("_")
                }
                backup_path_str = FallbackMonitor.save_audit_backup(
                    player_id=str(off.player_id),
                    type_name=type_name,
                    original_data=original_dict,
                    calculated_data=calc,
                    player_name=name,
                )
                backup_name = Path(backup_path_str).name
                if extra_fields_fn:
                    extra_fields_fn(off, calc, name)
                save_fn([calc])
                logger.info(f"      ✅ Fixed {name} in DB. (Backup: {backup_name})")
                fix_count += 1
            except AUDIT_EXCEPTIONS as e:
                logger.info(f"      ⚠️ Failed to fix {name}: {e}")
                logger.exception(f"Failed to fix {name} {type_name}")
        return fix_count

    @staticmethod
    def _run_audit(
        title: str,
        year: int,
        series: str,
        model_class,
        official_filter,
        calc_fn,
        keys_to_check: list[str],
        extra_fields: dict,
        threshold_rules: list,
        save_fn,
        extra_fields_fn,
        fix: bool,
        max_mismatches: int,
        **threshold_kwargs,
    ):
        logger.info(f"🕵️  Auditing {title} stats for {year} {series}...")
        with SessionLocal() as session:
            official_stats = session.query(model_class).filter(*official_filter(session)).all()
            if not official_stats:
                logger.info(f"   ⚠️ No official {title.lower()} stats found to compare.")
                return
            calc_map_or_fn = calc_fn(session, year, series)
            mismatches = StatAudit._collect_mismatches(
                official_stats, calc_map_or_fn, keys_to_check, extra_fields, session
            )
            mismatches_count = len(mismatches)
            if mismatches_count == 0:
                logger.info(f"   ✅ No {title.lower()} mismatches found.")
                return
            logger.info(f"   ❌ Found {mismatches_count} {title.lower()} mismatches.")
            for m in mismatches:
                logger.info(f"      - {m['name']} (ID:{m['player_id']}): {', '.join(m['diffs'])}")
            if not fix:
                logger.info("   ℹ️ Fix is disabled. Mismatches not resolved in DB.")
                StatAudit.send_audit_warning_alert(year, series, title.upper(), mismatches)
                return
            abort, reasons = StatAudit._safety_check(mismatches, max_mismatches, threshold_rules)
            if abort:
                reason_str = "; ".join(reasons)
                logger.info(f"   🛑 Auto-remediation ABORTED for {title.upper()}: {reason_str}")
                StatAudit.send_remediation_abort_alert(year, series, title.upper(), reason_str)
                return
            fix_count = StatAudit._fix_mismatches(mismatches, title.lower(), session, save_fn, extra_fields_fn)
            summary = f"Audited {len(official_stats)} records. Mismatches: {mismatches_count}, Fixed: {fix_count}"
            logger.info(f"   📊 {summary}")
            if fix_count > 0:
                fixed_players = [m for m in mismatches if m["name"] is not None][:fix_count]
                StatAudit.send_remediation_success_alert(year, series, title.upper(), mismatches_count, fixed_players)
            if mismatches_count > fix_count:
                FallbackMonitor.log_fallback(year, series, f"{title.upper()}_AUDIT", summary)

    @staticmethod
    def audit_batting(year: int, series: str, fix: bool = False, max_mismatches: int = 10, max_game_diff: int = 15):
        StatAudit._run_audit(
            "BATTING",
            year,
            series,
            PlayerSeasonBatting,
            lambda s: (
                PlayerSeasonBatting.season == year,
                PlayerSeasonBatting.league == series.upper(),
                PlayerSeasonBatting.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"]),
            ),
            lambda s, y, sr: {
                c["player_id"]: c
                for c in SeasonStatAggregator.aggregate_batting_season_bulk(s, y, sr, source="AUDIT_FIX")
            },
            ["games", "at_bats", "hits", "home_runs", "rbi", "walks"],
            {"off_games": ("games", "games")},
            [
                (
                    "off_games",
                    "games",
                    max_game_diff,
                    "Player {name} (ID:{pid}) has game difference of {diff} (Official: {off}, Calculated: {calc}), which exceeds threshold of {threshold}",
                )
            ],
            save_batting_stats_safe,
            lambda off, calc, name: (
                calc.update({"player_name": name}) or calc.setdefault("team_code", off.team_code),
            ),
            fix,
            max_mismatches,
        )

    @staticmethod
    def audit_pitching(year, series, fix=False, max_mismatches=10, max_game_diff=15, max_innings_outs_diff=45):
        StatAudit._run_audit(
            "PITCHING",
            year,
            series,
            PlayerSeasonPitching,
            lambda s: (
                PlayerSeasonPitching.season == year,
                PlayerSeasonPitching.league == series.upper(),
                PlayerSeasonPitching.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"]),
            ),
            lambda s, y, sr: {
                c["player_id"]: c
                for c in SeasonStatAggregator.aggregate_pitching_season_bulk(s, y, sr, source="AUDIT_FIX")
            },
            ["games", "wins", "losses", "saves", "earned_runs", "innings_outs"],
            {"off_games": ("games", "games"), "off_outs": ("innings_outs", "innings_outs")},
            [
                ("off_games", "games", max_game_diff, "Player {name} (ID:{pid}) has game difference of {diff} ..."),
                (
                    "off_outs",
                    "outs",
                    max_innings_outs_diff,
                    "Player {name} (ID:{pid}) has innings outs difference of {diff} ...",
                ),
            ],
            save_pitching_stats_to_db,
            lambda off, calc, name: (
                calc.update({"player_name": name}) or calc.setdefault("team_code", off.team_code),
            ),
            fix,
            max_mismatches,
        )

    @staticmethod
    def audit_fielding(year, series, fix=False, max_mismatches=15, max_error_diff=5):
        StatAudit._run_audit(
            "FIELDING",
            year,
            series,
            PlayerSeasonFielding,
            lambda s: (
                PlayerSeasonFielding.year == year,
                PlayerSeasonFielding.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"]),
            ),
            lambda s, y, sr: {
                pid: SeasonStatAggregator.aggregate_fielding_season(s, pid, y, sr, source="AUDIT_FIX")
                for pid in {
                    off.player_id for off in s.query(PlayerSeasonFielding).filter(PlayerSeasonFielding.year == y).all()
                }
            },
            ["errors"],
            {"off_errors": ("errors", "errors")},
            [("off_errors", "errors", max_error_diff, "Player {name} (ID:{pid}) has error difference of {diff} ...")],
            PlayerSeasonFieldingRepository().upsert_many,
            lambda off, calc, name: calc.update({"team_id": off.team_id}),
            fix,
            max_mismatches,
        )

    @staticmethod
    def audit_baserunning(year, series, fix=False, max_mismatches=15, max_sb_cs_diff=10):
        StatAudit._run_audit(
            "BASERUNNING",
            year,
            series,
            PlayerSeasonBaserunning,
            lambda s: (
                PlayerSeasonBaserunning.year == year,
                PlayerSeasonBaserunning.source.notin_(["FALLBACK", "FALLBACK_AUTO", "AUDIT_FIX", "MANUAL_RECALC"]),
            ),
            lambda s, y, sr: {
                c.get("player_id"): c
                for c in [
                    SeasonStatAggregator.aggregate_baserunning_season(s, off.player_id, y, sr, source="AUDIT_FIX")
                    for off in s.query(PlayerSeasonBaserunning).filter(PlayerSeasonBaserunning.year == y).all()
                ]
            },
            ["stolen_bases", "caught_stealing"],
            {"off_sb": ("stolen_bases", "stolen_bases"), "off_cs": ("caught_stealing", "caught_stealing")},
            [
                ("off_sb", "sb", max_sb_cs_diff, "Player {name} (ID:{pid}) has stolen bases difference of {diff} ..."),
                (
                    "off_cs",
                    "cs",
                    max_sb_cs_diff,
                    "Player {name} (ID:{pid}) has caught stealing difference of {diff} ...",
                ),
            ],
            PlayerSeasonBaserunningRepository().upsert_many,
            lambda off, calc, name: calc.update({"team_id": off.team_id}),
            fix,
            max_mismatches,
        )


def main():
    parser = argparse.ArgumentParser(description="Audit fallback aggregation accuracy.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--series", type=str, default="regular")
    parser.add_argument(
        "--type",
        type=str,
        default="all",
        choices=["batting", "pitching", "fielding", "baserunning", "all"],
    )
    parser.add_argument("--fix", action="store_true", help="Automatically fix mismatches in DB")
    parser.add_argument(
        "--max-mismatches",
        type=int,
        default=10,
        help="Max mismatches allowed before aborting auto-fix",
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
