"""
KBO Daily Data Quality Report Generator.
Analyzes daily data integrity and statistical consistency.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence
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
)
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.season import KboSeason
from src.utils.alerting import SlackWebhookClient
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.validators.quality_gate import run_quality_gate
from src.validators.standings_integrity import validate_standings_integrity

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
    rows = session.execute(
        select(table.c.player_id, table.c.name)
        .where(table.c.created_at >= day_start)
        .where(table.c.created_at <= day_end)
        .order_by(table.c.player_id)
    ).all()
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


def get_auto_remediation_summary(target_date_str: str, audit_dir: Path | None = None) -> dict[str, Any]:
    """
    Scans logs/audit_fixes/ for files starting with target_date_str.
    Parses warning, abort, and fixed player details to return a status summary.
    """
    import json
    from pathlib import Path

    if audit_dir is None:
        project_root = Path(__file__).resolve().parent.parent.parent
        audit_dir = project_root / "logs" / "audit_fixes"

    summary = {
        "status": "no_issues",
        "categories_fixed": [],
        "total_fixed": 0,
        "players_fixed": [],
        "categories_warning": [],
        "total_warning": 0,
        "players_warning": [],
        "categories_aborted": [],
        "abort_reasons": [],
    }

    if not audit_dir.exists():
        return summary

    has_abort = False
    has_warning = False
    has_fixed = False

    # List all files for target_date_str
    files = sorted(audit_dir.glob(f"{target_date_str}_*.json"))

    for f in files:
        filename = f.name
        try:
            with open(f, encoding="utf-8") as file_handle:
                content = json.load(file_handle)
        except SQLAlchemyError as e:
            _LOGGER.error(f"Failed to read/parse audit fix file {filename}: {e}")
            continue

        # Check file type based on naming pattern
        if "_abort_" in filename:
            has_abort = True
            category = filename.split("_")[-1].replace(".json", "").upper()
            if category not in summary["categories_aborted"]:
                summary["categories_aborted"].append(category)
            reason = content.get("reason", "unknown reason")
            summary["abort_reasons"].append(f"{category}: {reason}")

        elif "_warning_" in filename:
            has_warning = True
            category = filename.split("_")[-1].replace(".json", "").upper()
            if category not in summary["categories_warning"]:
                summary["categories_warning"].append(category)
            mismatches = content.get("mismatches", [])
            summary["total_warning"] += len(mismatches)
            for m in mismatches:
                summary["players_warning"].append(
                    {
                        "name": m.get("name"),
                        "player_id": m.get("player_id"),
                        "category": category,
                        "diffs": m.get("diffs", []),
                    }
                )

        else:
            # Fixed player snapshot {date}_{player_id}_{type}.json
            # Note: content is a list of snapshots
            has_fixed = True
            parts = filename.replace(".json", "").split("_")
            category = parts[-1].upper() if len(parts) >= 3 else "UNKNOWN"
            if category not in summary["categories_fixed"]:
                summary["categories_fixed"].append(category)

            snapshots = content if isinstance(content, list) else [content]
            summary["total_fixed"] += len(snapshots)
            for snap in snapshots:
                diffs = []
                orig = snap.get("original", {})
                calc = snap.get("calculated", {})
                player_id = snap.get("player_id")
                player_name = snap.get("player_name") or calc.get("player_name") or orig.get("player_name")

                for k in [
                    "games",
                    "at_bats",
                    "hits",
                    "home_runs",
                    "rbi",
                    "walks",
                    "wins",
                    "losses",
                    "saves",
                    "earned_runs",
                    "innings_outs",
                    "errors",
                    "stolen_bases",
                    "caught_stealing",
                ]:
                    if k in orig or k in calc:
                        o_val = orig.get(k)
                        c_val = calc.get(k)
                        if o_val != c_val:
                            diffs.append(f"{k}: {o_val}→{c_val}")

                summary["players_fixed"].append(
                    {"name": player_name, "player_id": player_id, "category": category, "diffs": diffs}
                )

    if has_abort:
        summary["status"] = "aborted"
    elif has_warning:
        summary["status"] = "warning"
    elif has_fixed:
        summary["status"] = "fixed"

    return summary


def get_pa_formula_integrity(session, year: int) -> dict[str, Any]:
    """Check PA = AB + BB + HBP + SH + SF consistency for the current season."""
    season_ids = [
        row[0]
        for row in session.execute(
            select(KboSeason.season_id)
            .where(KboSeason.season_year == year)
            .where(
                or_(
                    KboSeason.league_type_code == 0,
                    KboSeason.league_type_name.in_(_REGULAR_SEASON_NAMES),
                )
            )
        ).all()
    ]
    if not season_ids:
        return {"ok": True, "violation_count": 0, "violations": []}

    rows = session.execute(
        select(
            GameBattingStat.game_id,
            GameBattingStat.player_name,
            GameBattingStat.plate_appearances,
            GameBattingStat.at_bats,
            GameBattingStat.walks,
            GameBattingStat.hbp,
            GameBattingStat.sacrifice_hits,
            GameBattingStat.sacrifice_flies,
            Game.game_date,
        )
        .join(Game, Game.game_id == GameBattingStat.game_id)
        .where(Game.season_id.in_(season_ids))
        .where(Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
        .where(
            func.coalesce(GameBattingStat.plate_appearances, 0)
            != (
                func.coalesce(GameBattingStat.at_bats, 0)
                + func.coalesce(GameBattingStat.walks, 0)
                + func.coalesce(GameBattingStat.hbp, 0)
                + func.coalesce(GameBattingStat.sacrifice_hits, 0)
                + func.coalesce(GameBattingStat.sacrifice_flies, 0)
            )
        )
        .order_by(Game.game_date, GameBattingStat.game_id)
    ).all()

    violations = [
        {
            "game_id": r.game_id,
            "game_date": r.game_date.isoformat(),
            "player_name": r.player_name,
            "pa": r.plate_appearances,
            "at_bats": r.at_bats,
            "walks": r.walks,
            "hbp": r.hbp,
            "sh": r.sacrifice_hits,
            "sf": r.sacrifice_flies,
        }
        for r in rows
    ]

    return {
        "ok": len(violations) == 0,
        "violation_count": len(violations),
        "violations": violations[:20],
    }


def get_pa_formula_trend(session, months: int = 6) -> dict[str, Any]:
    """Get PA formula violation trend for the last N months."""
    start_date = datetime.now(_KST).date() - timedelta(days=months * 30)

    season_ids = [
        r[0]
        for r in session.execute(
            select(KboSeason.season_id).where(
                or_(
                    KboSeason.league_type_code == 0,
                    KboSeason.league_type_name.in_(_REGULAR_SEASON_NAMES),
                )
            )
        ).all()
    ]
    if not season_ids:
        return {"ok": True, "months": [], "direction": "stable"}

    rows = session.execute(
        select(
            GameBattingStat.plate_appearances,
            GameBattingStat.at_bats,
            GameBattingStat.walks,
            GameBattingStat.hbp,
            GameBattingStat.sacrifice_hits,
            GameBattingStat.sacrifice_flies,
            Game.game_date,
        )
        .join(Game, Game.game_id == GameBattingStat.game_id)
        .where(Game.season_id.in_(season_ids))
        .where(Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
        .where(Game.game_date >= start_date)
        .order_by(Game.game_date)
    ).all()

    monthly: dict[str, dict[str, int]] = {}
    for r in rows:
        month_key = r.game_date.strftime("%Y-%m")
        if month_key not in monthly:
            monthly[month_key] = {"total_checked": 0, "violation_count": 0}
        monthly[month_key]["total_checked"] += 1

        pa = r.plate_appearances or 0
        expected = (r.at_bats or 0) + (r.walks or 0) + (r.hbp or 0) + (r.sacrifice_hits or 0) + (r.sacrifice_flies or 0)
        if pa != expected:
            monthly[month_key]["violation_count"] += 1

    trend = [
        {
            "month": month,
            "total_checked": data["total_checked"],
            "violation_count": data["violation_count"],
            "violation_pct": round(data["violation_count"] / data["total_checked"] * 100, 4)
            if data["total_checked"] > 0
            else 0.0,
        }
        for month, data in sorted(monthly.items())
    ]

    direction = "stable"
    if len(trend) >= 2:
        recent = trend[-1]["violation_count"]
        prev = trend[-2]["violation_count"]
        if recent > prev:
            direction = "worsening"
        elif recent < prev:
            direction = "improving"

    return {
        "months": trend,
        "direction": direction,
        "ok": all(m["violation_count"] == 0 for m in trend),
    }


def get_daily_metrics(session, target_date_str: str) -> dict[str, Any]:
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

    # 4. Sabermetrics Highlights
    top_war_player = (
        session.query(PlayerBasic.name, PlayerSeasonBatting.extra_stats)
        .join(PlayerSeasonBatting, PlayerBasic.player_id == PlayerSeasonBatting.player_id)
        .filter(PlayerSeasonBatting.season == target_dt.year)
        .filter(PlayerSeasonBatting.league == "REGULAR")
        .all()
    )

    best_player = None
    if top_war_player:
        valid_players = []
        for name, extra in top_war_player:
            if extra and isinstance(extra, dict) and extra.get("war") is not None:
                valid_players.append({"name": name, "war": extra["war"]})
        if valid_players:
            best_player = max(valid_players, key=lambda x: x["war"])

    # 5. Data Parity (Local vs OCI)
    parity_info = {"ok": True, "local_count": 0, "oci_count": 0, "diff": 0}
    try:
        import os

        from sqlalchemy import create_engine, text

        local_count = session.query(func.count(Game.game_id)).scalar()
        parity_info["local_count"] = local_count

        target_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
        if target_url:
            oci_engine = create_engine(target_url)
            with oci_engine.connect() as conn:
                oci_count = conn.execute(text("SELECT count(*) FROM game")).scalar()
                parity_info["oci_count"] = oci_count
                parity_info["diff"] = oci_count - local_count
                parity_info["ok"] = parity_info["diff"] == 0
    except SQLAlchemyError as e:
        _LOGGER.error(f"Parity check failed: {e}")
        parity_info["ok"] = False
        parity_info["error"] = str(e)

    # 6. Auto-Remediation Summary
    auto_remediation = get_auto_remediation_summary(target_date_str)

    # 7. PA Formula Integrity
    pa_formula_integrity = get_pa_formula_integrity(session, target_dt.year)

    # 8. PA Formula Trend (6-month)
    pa_formula_trend = get_pa_formula_trend(session, months=6)

    return {
        "date": target_date_str,
        "status_counts": status_map,
        "detail_integrity": detail_integrity,
        "new_players": new_players,
        "relay_integrity": relay_integrity,
        "standings_integrity": standings_integrity,
        "top_performer": best_player,
        "parity": parity_info,
        "total_games": sum(status_map.values()),
        "completed_count": len(game_ids),
        "auto_remediation": auto_remediation,
        "pa_formula_integrity": pa_formula_integrity,
        "pa_formula_trend": pa_formula_trend,
    }


def format_telegram_report(metrics: dict[str, Any], gate_result: dict[str, Any]) -> str:
    """Format the metrics and gate results into a readable Telegram message."""
    parity = metrics.get("parity") or {}
    parity_icon = "✅" if parity.get("ok", True) else "🚨"

    lines = [f"{parity_icon} <b>KBO Quality Report ({metrics['date']})</b>\n"]

    # Collection Status
    total = metrics["total_games"]
    comp = metrics["completed_count"]
    status_summary = ", ".join([f"{s}: {c}" for s, c in metrics["status_counts"].items()])
    lines.append(f"📡 <b>Collection</b>: {comp}/{total} games finished")
    lines.append(f"   ({status_summary})")

    # Parity Check
    if not parity.get("ok", True):
        lines.append(
            f"❓ <b>Parity</b>: Local {parity.get('local_count')} / OCI {parity.get('oci_count')} (Diff: {parity.get('diff')})"
        )

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
        if bat_miss:
            lines.append(f"   - Batting: {bat_miss} issues")
        if pit_miss:
            lines.append(f"   - Pitching: {pit_miss} issues")

    # Sabermetrics highlight
    top = metrics.get("top_performer")
    if top:
        lines.append(f"🔥 <b>Top Performer</b>: {top['name']} (WAR: {top['war']})")

    relay_integrity = metrics.get("relay_integrity") or {}
    if relay_integrity.get("ok", True):
        lines.append("✅ <b>PBP</b>: Recent/current-season relay complete")
    else:
        recent_count = relay_integrity.get("recent_missing_count", 0)
        season_count = relay_integrity.get("current_season_missing_count", 0)
        lines.append(f"⚠️ <b>PBP</b>: {recent_count} recent / {season_count} current-season games missing")
        for gid in list(relay_integrity.get("missing_game_ids") or [])[:5]:
            lines.append(f"   - {gid}")

    standings_integrity = metrics.get("standings_integrity") or {}
    if standings_integrity.get("ok", True):
        lines.append("✅ <b>Standings</b>: Matches completed-game rollup")
    else:
        mismatch_count = len(standings_integrity.get("mismatches") or [])
        missing_score_count = len(standings_integrity.get("missing_score_games") or [])
        lines.append(f"❌ <b>Standings</b>: {mismatch_count} mismatches / {missing_score_count} score gaps")
        for item in list(standings_integrity.get("mismatches") or [])[:5]:
            team_code = item.get("team_code", "unknown")
            issue = item.get("issue", "mismatch")
            lines.append(f"   - {team_code}: {issue}")

    # Auto-Remediation Status
    auto_rem = metrics.get("auto_remediation") or {}
    status = auto_rem.get("status", "no_issues")
    if status == "fixed":
        cat_counts = {}
        for p in auto_rem.get("players_fixed", []):
            cat = p.get("category", "UNKNOWN").lower()
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
        cat_str = ", ".join([f"{k} {v}" for k, v in cat_counts.items()])
        lines.append(f"🔧 <b>Auto-Remediation</b>: {auto_rem.get('total_fixed')}건 수정 완료 ({cat_str})")
        for p in auto_rem.get("players_fixed", [])[:3]:
            diffs_str = ", ".join(p.get("diffs", [])[:2])
            lines.append(f"   - {p['name']} ({p['category']}): {diffs_str}")
        if len(auto_rem.get("players_fixed", [])) > 3:
            lines.append(f"   - ... 외 {len(auto_rem['players_fixed']) - 3}명")
    elif status == "warning":
        cat_counts = {}
        for p in auto_rem.get("players_warning", []):
            cat = p.get("category", "UNKNOWN").lower()
            cat_counts[cat] = cat_counts.get(cat, 0) + 1
        cat_str = ", ".join([f"{k} {v}" for k, v in cat_counts.items()])
        lines.append(
            f"⚠️ <b>Auto-Remediation</b>: mismatch {auto_rem.get('total_warning')}건 발견 (수정 비활성화) ({cat_str})"
        )
        for p in auto_rem.get("players_warning", [])[:3]:
            diffs_str = ", ".join(p.get("diffs", [])[:2])
            lines.append(f"   - {p['name']} ({p['category']}): {diffs_str}")
        if len(auto_rem.get("players_warning", [])) > 3:
            lines.append(f"   - ... 외 {len(auto_rem['players_warning']) - 3}명")
    elif status == "aborted":
        cats_str = ", ".join(auto_rem.get("categories_aborted", []))
        lines.append(f"🛑 <b>Auto-Remediation</b>: 작업 중단 ({cats_str})")
        for r in auto_rem.get("abort_reasons", [])[:3]:
            lines.append(f"   - {r}")
    else:
        lines.append("✅ <b>Auto-Remediation</b>: No issues detected")

    # PA Formula Integrity
    pa_formula = metrics.get("pa_formula_integrity") or {}
    if pa_formula.get("ok", True):
        lines.append("✅ <b>PA Formula</b>: All consistent (PA=AB+BB+HBP+SH+SF)")
    else:
        count = pa_formula.get("violation_count", 0)
        lines.append(f"❌ <b>PA Formula</b>: {count} violations")
        for v in (pa_formula.get("violations") or [])[:3]:
            lines.append(f"   - {v['game_date']} {v['player_name']} PA={v['pa']} ≠ AB+BB+HBP+SH+SF")

    # PA Formula Trend
    trend = metrics.get("pa_formula_trend") or {}
    if trend.get("months"):
        direction_icon = (
            "📈" if trend.get("direction") == "worsening" else "📉" if trend.get("direction") == "improving" else "📊"
        )
        lines.append(f"{direction_icon} <b>PA Formula Trend</b> (6mo): {trend['direction']}")
        for m in trend["months"][-4:]:
            icon = "❌" if m["violation_count"] > 0 else "✅"
            lines.append(f"   {icon} {m['month']}: {m['violation_count']}/{m['total_checked']} ({m['violation_pct']}%)")

    # New Players
    if metrics["new_players"]:
        p_names = ", ".join([p["name"] for p in metrics["new_players"][:5]])
        count = len(metrics["new_players"])
        lines.append(f"🆕 <b>New Players</b>: {count} found ({p_names})")

    return "\n".join(lines)


def _has_report_issues(metrics: dict[str, Any], gate_result: dict[str, Any]) -> bool:
    trend = metrics.get("pa_formula_trend") or {}
    return (
        not gate_result["ok"]
        or any(not d["is_complete"] for d in metrics["detail_integrity"])
        or not (metrics.get("relay_integrity") or {}).get("ok", True)
        or not (metrics.get("standings_integrity") or {}).get("ok", True)
        or metrics.get("auto_remediation", {}).get("status") in ("warning", "aborted")
        or not (metrics.get("pa_formula_integrity") or {}).get("ok", True)
        or trend.get("direction") == "worsening"
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

    report_json = {"metrics": metrics, "quality_gate": gate_result, "generated_at": datetime.now(_KST).isoformat()}

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
