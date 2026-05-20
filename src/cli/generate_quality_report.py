"""
KBO Daily Data Quality Report Generator.
Analyzes daily data integrity and statistical consistency.
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import MetaData, Table, func, inspect, or_, select
from sqlalchemy.exc import SQLAlchemyError
from src.db.engine import SessionLocal
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.models.season import KboSeason
from src.validators.quality_gate import run_quality_gate
from src.validators.standings_integrity import validate_standings_integrity
from src.utils.alerting import SlackWebhookClient
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


_KST = ZoneInfo("Asia/Seoul")
_LOGGER = logging.getLogger(__name__)
_PLAYER_BASIC_NEW_PLAYER_COLUMNS = {"player_id", "name", "created_at"}
_REGULAR_SEASON_NAMES = ("정규시즌", "Regular Season", "regular")


def _player_basic_table_for_new_players(session) -> Table | None:
    """Return a player_basic table object when new-player dates are queryable."""
    table = PlayerBasic.__table__
    table_name = table.name
    bind = session.get_bind()

    db_columns: set[str] | None = None
    if bind is not None:
        try:
            db_columns = {column["name"] for column in inspect(bind).get_columns(table_name)}
        except SQLAlchemyError as exc:
            _LOGGER.info("Could not inspect %s for new player report metrics: %s", table_name, exc)

    if db_columns is not None and not _PLAYER_BASIC_NEW_PLAYER_COLUMNS.issubset(db_columns):
        _LOGGER.info("Omitting new_players metric: %s.created_at is unavailable", table_name)
        return None

    if _PLAYER_BASIC_NEW_PLAYER_COLUMNS.issubset(table.c.keys()):
        return table

    if bind is None:
        _LOGGER.info("Omitting new_players metric: no database bind is available")
        return None

    try:
        reflected_table = Table(table_name, MetaData(), autoload_with=bind)
    except SQLAlchemyError as exc:
        _LOGGER.info("Could not reflect %s for new player report metrics: %s", table_name, exc)
        return None

    if _PLAYER_BASIC_NEW_PLAYER_COLUMNS.issubset(reflected_table.c.keys()):
        return reflected_table

    _LOGGER.info("Omitting new_players metric: %s.created_at is unavailable", table_name)
    return None


def _get_new_players(session, target_dt: date) -> list[dict[str, Any]]:
    table = _player_basic_table_for_new_players(session)
    if table is None:
        return []

    day_start = datetime.combine(target_dt, datetime.min.time())
    day_end = datetime.combine(target_dt, datetime.max.time())
    rows = (
        session.execute(
            select(table.c.player_id, table.c.name)
            .where(table.c.created_at >= day_start)
            .where(table.c.created_at <= day_end)
            .order_by(table.c.player_id)
        )
        .all()
    )
    return [{"id": player_id, "name": name} for player_id, name in rows]


def get_relay_integrity_metrics(
    session,
    target_date: date,
    recent_days: int = 14,
) -> dict[str, Any]:
    """Return completed games missing game_play_by_play rows."""
    recent_start = target_date - timedelta(days=max(recent_days - 1, 0))
    pbp_game_ids = select(GamePlayByPlay.game_id).distinct()

    recent_missing = [
        row[0]
        for row in (
            session.query(Game.game_id)
            .filter(
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.game_date >= recent_start,
                Game.game_date <= target_date,
                ~Game.game_id.in_(pbp_game_ids),
            )
            .order_by(Game.game_date.asc(), Game.game_id.asc())
            .all()
        )
    ]

    current_season_missing = [
        row[0]
        for row in (
            session.query(Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(
                KboSeason.season_year == target_date.year,
                or_(
                    KboSeason.league_type_code == 0,
                    KboSeason.league_type_name.in_(_REGULAR_SEASON_NAMES),
                ),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                ~Game.game_id.in_(pbp_game_ids),
            )
            .order_by(Game.game_date.asc(), Game.game_id.asc())
            .all()
        )
    ]
    missing_game_ids = sorted(set(recent_missing) | set(current_season_missing))

    return {
        "ok": not missing_game_ids,
        "checked_date": target_date.isoformat(),
        "recent_days": recent_days,
        "recent_missing_count": len(recent_missing),
        "current_season_missing_count": len(current_season_missing),
        "missing_game_ids": missing_game_ids,
        "recent_missing_game_ids": recent_missing,
        "current_season_missing_game_ids": current_season_missing,
    }


def get_daily_metrics(session, target_date_str: str) -> Dict[str, Any]:
    """Calculate core collection metrics for a specific date."""
    target_dt = datetime.strptime(target_date_str, "%Y%m%d").date()
    
    # 1. Game Status Counts
    status_counts = (
        session.query(Game.game_status, func.count(Game.game_id))
        .filter(Game.game_date == target_dt)
        .group_by(Game.game_status)
        .all()
    )
    status_map = {status: count for status, count in status_counts}
    
    # 2. Detail Completion Analysis
    completed_games = (
        session.query(Game.game_id)
        .filter(Game.game_date == target_dt)
        .filter(Game.game_status.in_(["COMPLETED", "DRAW"]))
        .all()
    )
    game_ids = [g[0] for g in completed_games]
    
    detail_integrity = []
    for gid in game_ids:
        metrics = {
            "game_id": gid,
            "has_metadata": session.query(GameMetadata).filter_by(game_id=gid).count() > 0,
            "has_innings": session.query(GameInningScore).filter_by(game_id=gid).count() > 0,
            "has_lineup": session.query(GameLineup).filter_by(game_id=gid).count() > 0,
            "has_batting": session.query(GameBattingStat).filter_by(game_id=gid).count() > 0,
            "has_pitching": session.query(GamePitchingStat).filter_by(game_id=gid).count() > 0,
            "has_pbp": session.query(GamePlayByPlay).filter_by(game_id=gid).count() > 0,
        }
        metrics["is_complete"] = all(metrics.values())
        detail_integrity.append(metrics)
    
    # 3. New Players
    new_players = _get_new_players(session, target_dt)
    relay_integrity = get_relay_integrity_metrics(session, target_dt)
    standings_integrity = validate_standings_integrity(session, target_dt)
    
    return {
        "date": target_date_str,
        "status_counts": status_map,
        "detail_integrity": detail_integrity,
        "new_players": new_players,
        "relay_integrity": relay_integrity,
        "standings_integrity": standings_integrity,
        "total_games": sum(status_map.values()),
        "completed_count": len(game_ids)
    }


def format_telegram_report(metrics: Dict[str, Any], gate_result: Dict[str, Any]) -> str:
    """Format the metrics and gate results into a readable Telegram message."""
    lines = [f"<b>📊 KBO Quality Report ({metrics['date']})</b>\n"]
    
    # Collection Status
    total = metrics["total_games"]
    comp = metrics["completed_count"]
    status_summary = ", ".join([f"{s}: {c}" for s, c in metrics["status_counts"].items()])
    lines.append(f"📡 <b>Collection</b>: {comp}/{total} games finished")
    lines.append(f"   ({status_summary})")
    
    # Detail Integrity
    incomplete = [d["game_id"] for d in metrics["detail_integrity"] if not d["is_complete"]]
    if not incomplete:
        lines.append("✅ <b>Integrity</b>: 100% (All details captured)")
    else:
        lines.append(f"⚠️ <b>Integrity</b>: {len(incomplete)} games missing details")
        for gid in incomplete[:3]:
            lines.append(f"   - {gid}")
    
    # Statistical Consistency
    if gate_result["ok"]:
        lines.append("✅ <b>Stats</b>: Consistent with cumulative totals")
    else:
        bat_miss = len(gate_result["batting"].get("mismatches", []))
        pit_miss = len(gate_result["pitching"].get("mismatches", []))
        lines.append(f"❌ <b>Stats</b>: {bat_miss + pit_miss} mismatches detected")
        if bat_miss: lines.append(f"   - Batting: {bat_miss} issues")
        if pit_miss: lines.append(f"   - Pitching: {pit_miss} issues")

    relay_integrity = metrics.get("relay_integrity") or {}
    if relay_integrity.get("ok", True):
        lines.append("✅ <b>PBP</b>: Recent/current-season relay complete")
    else:
        recent_count = relay_integrity.get("recent_missing_count", 0)
        season_count = relay_integrity.get("current_season_missing_count", 0)
        lines.append(
            "⚠️ <b>PBP</b>: "
            f"{recent_count} recent / {season_count} current-season games missing"
        )
        for gid in list(relay_integrity.get("missing_game_ids") or [])[:5]:
            lines.append(f"   - {gid}")

    standings_integrity = metrics.get("standings_integrity") or {}
    if standings_integrity.get("ok", True):
        lines.append("✅ <b>Standings</b>: Matches completed-game rollup")
    else:
        mismatch_count = len(standings_integrity.get("mismatches") or [])
        missing_score_count = len(standings_integrity.get("missing_score_games") or [])
        lines.append(
            "❌ <b>Standings</b>: "
            f"{mismatch_count} mismatches / {missing_score_count} score gaps"
        )
        for item in list(standings_integrity.get("mismatches") or [])[:5]:
            team_code = item.get("team_code", "unknown")
            issue = item.get("issue", "mismatch")
            lines.append(f"   - {team_code}: {issue}")
        
    # New Players
    if metrics["new_players"]:
        p_names = ", ".join([p["name"] for p in metrics["new_players"][:5]])
        count = len(metrics["new_players"])
        lines.append(f"🆕 <b>New Players</b>: {count} found ({p_names})")
    
    return "\n".join(lines)


def _has_report_issues(metrics: dict[str, Any], gate_result: dict[str, Any]) -> bool:
    return (
        not gate_result["ok"]
        or any(not d["is_complete"] for d in metrics["detail_integrity"])
        or not (metrics.get("relay_integrity") or {}).get("ok", True)
        or not (metrics.get("standings_integrity") or {}).get("ok", True)
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate KBO Daily Quality Report")
    parser.add_argument("--date", type=str, help="Target date YYYYMMDD (defaults to today)")
    parser.add_argument("--notify", action="store_true", help="Send to Telegram if issues found")
    parser.add_argument("--force-notify", action="store_true", help="Always send to Telegram")
    args = parser.parse_args(argv)

    target_date = args.date if args.date else datetime.now(_KST).strftime("%Y%m%d")
    year = int(target_date[:4])
    
    with SessionLocal() as session:
        metrics = get_daily_metrics(session, target_date)
        gate_result = run_quality_gate(session, year)
        
    report_json = {
        "metrics": metrics,
        "quality_gate": gate_result,
        "generated_at": datetime.now(_KST).isoformat()
    }
    
    # Save to logs
    log_dir = Path("logs/quality_reports")
    log_dir.mkdir(parents=True, exist_ok=True)
    with open(log_dir / f"{target_date}.json", "w", encoding="utf-8") as f:
        json.dump(report_json, f, indent=2, ensure_ascii=False)
        
    print(f"✅ Quality report saved to {log_dir}/{target_date}.json")
    
    # Telegram Notification
    telegram_msg = format_telegram_report(metrics, gate_result)
    
    should_notify = args.force_notify or (args.notify and _has_report_issues(metrics, gate_result))
    
    if should_notify:
        print("🚀 Sending report to Telegram...")
        SlackWebhookClient.send_alert(telegram_msg)
    else:
        print("\n" + telegram_msg.replace("<b>", "").replace("</b>", ""))
        
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
