"""
Unified data gap reporter for KBO pipeline.

Aggregates gap checks from multiple existing monitors into a single
structured report, then sends gap-type-aware alerts via Slack/Telegram.

Usage:
    python -m src.cli.gap_report                     # run + alert
    python -m src.cli.gap_report --no-alert           # run only
    python -m src.cli.gap_report --dry-run            # print only
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select, text
from sqlalchemy.exc import SQLAlchemyError

from src.cli.freshness_gate import collect_freshness_issues
from src.cli.monitor_data_freshness import check_freshness
from src.db.engine import SessionLocal
from src.models.game import GamePlayByPlay
from src.utils.alerting import GAP_EMOJI_MAP, SlackWebhookClient
from src.validators.standings_integrity import validate_standings_integrity

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")


def check_relay_gaps() -> dict[str, Any]:
    """Find COMPLETED/DRAW games missing game_play_by_play (last 14 days)."""
    from src.models.game import Game
    from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

    start = datetime.now(KST).date() - timedelta(days=14)
    relay_games: list[str] = []
    with SessionLocal() as session:
        stmt = select(Game.game_id).where(
            Game.game_date >= start,
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            ~Game.game_id.in_(select(GamePlayByPlay.game_id).distinct()),
        )
        relay_games = list(session.execute(stmt).scalars().all())
    return {
        "ok": len(relay_games) == 0,
        "missing_count": len(relay_games),
        "missing_game_ids": relay_games,
    }


def check_profile_gaps() -> dict[str, Any]:
    """Find player IDs missing photo_url (excludes pseudo/not-found)."""
    from src.models.player import PlayerBasic

    with SessionLocal() as session:
        rows = (
            session.query(PlayerBasic.player_id)
            .filter(
                PlayerBasic.photo_url.is_(None),
                PlayerBasic.player_id >= 10000,
                or_(PlayerBasic.status.is_(None), ~PlayerBasic.status.in_(["NOT_FOUND", "PSEUDO"])),
            )
            .all()
        )
    player_ids = [r.player_id for r in rows]
    return {
        "ok": len(player_ids) == 0,
        "missing_count": len(player_ids),
        "missing_player_ids": player_ids[:20],
    }


def check_id_resolution_gaps() -> dict[str, Any]:
    """Find NULL player_ids in game stats tables."""
    with SessionLocal() as session:
        batting = session.execute(text("SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL")).scalar() or 0
        pitching = (
            session.execute(text("SELECT COUNT(*) FROM game_pitching_stats WHERE player_id IS NULL")).scalar() or 0
        )
        lineups = session.execute(text("SELECT COUNT(*) FROM game_lineups WHERE player_id IS NULL")).scalar() or 0
    total = batting + pitching + lineups
    counts = {"batting": batting, "pitching": pitching, "lineups": lineups}
    return {"ok": total == 0, "total": total, "counts": counts}


def check_pa_formula_gaps() -> dict[str, Any]:
    """Find PA formula violations (PA != AB+BB+HBP+SH+SF) for the current season."""
    from src.cli.generate_quality_report import get_pa_formula_integrity

    year = datetime.now(KST).year
    with SessionLocal() as session:
        pa_formula = get_pa_formula_integrity(session, year)
    return {
        "ok": pa_formula.get("ok", True),
        "violation_count": pa_formula.get("violation_count", 0),
        "violations": pa_formula.get("violations", [])[:20],
    }


def check_team_stats_gaps() -> dict[str, Any]:
    """Check TeamSeasonBatting/Pitching vs PlayerSeasonBatting/Pitching consistency."""
    year = datetime.now(KST).year
    with SessionLocal() as session:
        from src.validators.quality_gate import run_quality_gate

        gate = run_quality_gate(session, year)
        team_batting = gate.get("team_batting", {})
        team_pitching = gate.get("team_pitching", {})
        batting_mismatches = len(team_batting.get("mismatches", []))
        pitching_mismatches = len(team_pitching.get("mismatches", []))
        return {
            "ok": team_batting.get("ok", True) and team_pitching.get("ok", True),
            "batting_mismatches": batting_mismatches,
            "pitching_mismatches": pitching_mismatches,
            "total": batting_mismatches + pitching_mismatches,
            "details": {
                "batting": team_batting.get("mismatches", []),
                "pitching": team_pitching.get("mismatches", []),
            },
        }


def build_gap_report() -> dict[str, Any]:
    """Run all gap checks and return a unified report dict."""
    report: dict[str, Any] = {
        "generated_at": datetime.now(KST).isoformat(),
        "gaps": {},
    }

    # 1. Freshness gaps (per-game data completeness)
    try:
        with SessionLocal() as session:
            freshness = collect_freshness_issues(session, days=3)
        total_issues = sum(len(v) for v in freshness.values())
        report["gaps"]["FRESHNESS"] = {
            "ok": total_issues == 0,
            "total_issues": total_issues,
            "details": {k: v for k, v in freshness.items() if v},
        }
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("FRESHNESS gap check failed")
        report["gaps"]["FRESHNESS"] = {"ok": False, "error": str(e)}

    # 2. Relay/PBP gaps
    try:
        report["gaps"]["RELAY"] = check_relay_gaps()
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("RELAY gap check failed")
        report["gaps"]["RELAY"] = {"ok": False, "error": str(e)}

    # 3. Source staleness
    try:
        stale = check_freshness(dry_run=True)
        report["gaps"]["STALENESS"] = {
            "ok": len(stale) == 0,
            "stale_count": len(stale),
            "details": stale,
        }
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("STALENESS gap check failed")
        report["gaps"]["STALENESS"] = {"ok": False, "error": str(e)}

    # 4. Standings integrity
    try:
        target_date = datetime.now(KST).date() - timedelta(days=1)
        with SessionLocal() as session:
            standings = validate_standings_integrity(session, target_date)
        report["gaps"]["STANDINGS"] = {
            "ok": standings.get("ok", False),
            "mismatches": len(standings.get("mismatches", [])),
            "missing_scores": len(standings.get("missing_score_games", [])),
        }
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("STANDINGS gap check failed")
        report["gaps"]["STANDINGS"] = {"ok": False, "error": str(e)}

    # 5. Player profile gaps
    try:
        report["gaps"]["PROFILE"] = check_profile_gaps()
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("PROFILE gap check failed")
        report["gaps"]["PROFILE"] = {"ok": False, "error": str(e)}

    # 6. Player ID resolution gaps
    try:
        report["gaps"]["ID_RESOLUTION"] = check_id_resolution_gaps()
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("ID_RESOLUTION gap check failed")
        report["gaps"]["ID_RESOLUTION"] = {"ok": False, "error": str(e)}

    # 7. PA formula gaps
    try:
        report["gaps"]["PA_FORMULA"] = check_pa_formula_gaps()
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("PA_FORMULA gap check failed")
        report["gaps"]["PA_FORMULA"] = {"ok": False, "error": str(e)}

    # 8. Team stats consistency
    try:
        report["gaps"]["TEAM_STATS"] = check_team_stats_gaps()
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as e:
        logger.exception("TEAM_STATS gap check failed")
        report["gaps"]["TEAM_STATS"] = {"ok": False, "error": str(e)}

    return report


def _gap_severity(gap: dict[str, Any]) -> str:
    if gap.get("error"):
        return "error"
    if not gap.get("ok", True):
        return "warning"
    return "ok"


def _freshness_summary_parts(gap_data: dict[str, Any]) -> list[str]:
    details = gap_data.get("details", {})
    summary_parts = [f"{gap_data.get('total_issues', 0)} total issues"]
    for key, value in details.items():
        if value:
            summary_parts.append(f"{key}: {len(value)} games")
    return summary_parts


def _team_stats_summary_parts(gap_data: dict[str, Any]) -> list[str]:
    summary_parts = [f"{gap_data.get('total', 0)} team stat mismatches"]
    bat = gap_data.get("batting_mismatches", 0)
    pit = gap_data.get("pitching_mismatches", 0)
    if bat:
        summary_parts.append(f"batting={bat}")
    if pit:
        summary_parts.append(f"pitching={pit}")
    return summary_parts


def _gap_summary_parts(gap_type: str, gap_data: dict[str, Any]) -> list[str]:
    if gap_type == "FRESHNESS":
        return _freshness_summary_parts(gap_data)
    if gap_type == "RELAY":
        return [f"{gap_data.get('missing_count', 0)} games missing PBP"]
    if gap_type == "STALENESS":
        return [f"{gap_data.get('stale_count', 0)} stale sources"]
    if gap_type == "STANDINGS":
        return [f"{gap_data.get('mismatches', 0)} mismatches, {gap_data.get('missing_scores', 0)} missing scores"]
    if gap_type == "PROFILE":
        return [f"{gap_data.get('missing_count', 0)} players missing profiles"]
    if gap_type == "ID_RESOLUTION":
        counts = gap_data.get("counts", {})
        return [
            f"{gap_data.get('total', 0)} NULL player_ids (batting={counts.get('batting')}, pitching={counts.get('pitching')}, lineups={counts.get('lineups')})",
        ]
    if gap_type == "PA_FORMULA":
        return [f"{gap_data.get('violation_count', 0)} PA formula violations"]
    if gap_type == "TEAM_STATS":
        return _team_stats_summary_parts(gap_data)
    if gap_data.get("error"):
        return [f"Error: {gap_data['error']}"]
    return []


def _freshness_detail_items(gap_data: dict[str, Any]) -> list[str]:
    detail_items = []
    details = gap_data.get("details", {})
    for key, value in details.items():
        detail_items.extend(f"{key}: {game_id}" for game_id in value[:3])
    return detail_items


def _pa_formula_detail_items(gap_data: dict[str, Any]) -> list[str]:
    return [
        f"{violation['game_date']} {violation['player_name']} PA={violation['pa']} ≠ AB+BB+HBP+SH+SF"
        for violation in (gap_data.get("violations") or [])[:5]
    ]


def _team_stats_detail_items(gap_data: dict[str, Any]) -> list[str]:
    detail_items = []
    details = gap_data.get("details", {})
    for mismatch in (details.get("batting") or [])[:3]:
        team_id = mismatch.get("team_id", "?")
        detail_items.append(f"타격 [{team_id}]: {mismatch.get('issue', '')}")
        detail_items.extend(f"  {diff}" for diff in (mismatch.get("diffs") or [])[:2])
    for mismatch in (details.get("pitching") or [])[:3]:
        team_id = mismatch.get("team_id", "?")
        detail_items.append(f"투수 [{team_id}]: {mismatch.get('issue', '')}")
        detail_items.extend(f"  {diff}" for diff in (mismatch.get("diffs") or [])[:2])
    return detail_items


def _gap_detail_items(gap_type: str, gap_data: dict[str, Any]) -> list[str]:
    if gap_type == "RELAY":
        return [f"{gid}" for gid in (gap_data.get("missing_game_ids") or [])[:5]]
    if gap_type == "PROFILE":
        return [f"player_id={pid}" for pid in (gap_data.get("missing_player_ids") or [])[:5]]
    if gap_type == "STALENESS":
        return gap_data.get("details", [])[:5]
    if gap_type == "FRESHNESS":
        return _freshness_detail_items(gap_data)
    if gap_type == "PA_FORMULA":
        return _pa_formula_detail_items(gap_data)
    if gap_type == "TEAM_STATS":
        return _team_stats_detail_items(gap_data)
    return []


def send_gap_alerts(report: dict[str, Any]) -> None:
    """Send gap-type-aware alerts for each non-ok gap in the report."""
    for gap_type, gap_data in report.get("gaps", {}).items():
        severity = _gap_severity(gap_data)
        if severity == "ok":
            continue

        summary_parts = _gap_summary_parts(gap_type, gap_data)
        summary = ", ".join(summary_parts) if summary_parts else "Unknown gap"
        detail_items = _gap_detail_items(gap_type, gap_data)
        SlackWebhookClient.send_gap_alert(gap_type, summary, detail_items)


def format_report_summary(report: dict[str, Any]) -> str:
    """Return a human-readable one-line summary of all gap states."""
    parts = []
    for gap_type, gap_data in report.get("gaps", {}).items():
        emoji = GAP_EMOJI_MAP.get(gap_type, "\u2753")
        sev = _gap_severity(gap_data)
        icon = "\u2705" if sev == "ok" else "\u26a0\ufe0f" if sev == "warning" else "\u274c"
        parts.append(f"{icon}{emoji}{gap_type}")
    return " | ".join(parts)


def run_gap_report(alert: bool = True, dry_run: bool = False) -> dict[str, Any]:
    """Build and optionally alert the unified gap report."""
    if dry_run:
        logger.info("[GAP-REPORT] DRY RUN — no alerts will be sent")

    report = build_gap_report()
    summary = format_report_summary(report)
    logger.info("[GAP-REPORT] %s", summary)

    for gap_type, gap_data in report.get("gaps", {}).items():
        sev = _gap_severity(gap_data)
        emoji = GAP_EMOJI_MAP.get(gap_type, "\u2753")
        icon = "\u2705" if sev == "ok" else "\u26a0\ufe0f" if sev == "warning" else "\u274c"
        count = ""
        if "missing_count" in gap_data:
            count = f" ({gap_data['missing_count']})"
        elif "total_issues" in gap_data:
            count = f" ({gap_data['total_issues']})"
        elif "stale_count" in gap_data:
            count = f" ({gap_data['stale_count']})"
        elif "total" in gap_data:
            count = f" ({gap_data['total']})"
        elif "violation_count" in gap_data:
            count = f" ({gap_data['violation_count']})"
        logger.info("  %s %s %s: %s%s", icon, emoji, gap_type, sev, count)

    if alert and not dry_run:
        send_gap_alerts(report)

    return report


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KBO Unified Data Gap Report")
    parser.add_argument("--no-alert", action="store_true", help="Suppress Slack/Telegram alerts")
    parser.add_argument("--dry-run", action="store_true", help="Print only, no alerts")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run_gap_report(alert=not args.no_alert, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
