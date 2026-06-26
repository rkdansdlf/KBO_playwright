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
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from sqlalchemy import MetaData, Table, func, inspect, or_, select
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal, get_oci_url
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
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.validators.quality_gate import run_quality_gate
from src.validators.standings_integrity import validate_standings_integrity

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_LOGGER = logging.getLogger(__name__)
_PLAYER_BASIC_NEW_PLAYER_COLUMNS = {"player_id", "name", "created_at"}
_REGULAR_SEASON_NAMES = ("정규시즌", "Regular Season", "regular")


def _player_basic_table_for_new_players(session: Session) -> Table | None:
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


def _get_new_players(session: Session, target_dt: date) -> list[dict[str, Any]]:
    table = _player_basic_table_for_new_players(session)
    if table is None:
        return []

    day_start = datetime.combine(target_dt, datetime.min.time())
    day_end = datetime.combine(target_dt, datetime.max.time())
    rows = session.execute(
        select(table.c.player_id, table.c.name)
        .where(table.c.created_at >= day_start)
        .where(table.c.created_at <= day_end)
        .order_by(table.c.player_id),
    ).all()
    return [{"id": player_id, "name": name} for player_id, name in rows]


def get_relay_integrity_metrics(
    session: Session,
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


_AUTO_REMEDIATION_DIFF_KEYS = [
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
]


def _default_audit_fix_dir() -> Path:
    project_root = Path(__file__).resolve().parent.parent.parent
    return project_root / "logs" / "audit_fixes"


def _empty_auto_remediation_summary() -> dict[str, Any]:
    return {
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


def _add_unique_category(summary: dict[str, Any], key: str, category: str) -> None:
    if category not in summary[key]:
        summary[key].append(category)


def _audit_category_from_filename(filename: str) -> str:
    return filename.rsplit("_", maxsplit=1)[-1].replace(".json", "").upper()


def _record_auto_remediation_abort(summary: dict[str, Any], filename: str, content: dict[str, Any]) -> None:
    category = _audit_category_from_filename(filename)
    _add_unique_category(summary, "categories_aborted", category)
    reason = content.get("reason", "unknown reason")
    summary["abort_reasons"].append(f"{category}: {reason}")


def _record_auto_remediation_warning(summary: dict[str, Any], filename: str, content: dict[str, Any]) -> None:
    category = _audit_category_from_filename(filename)
    _add_unique_category(summary, "categories_warning", category)
    mismatches = content.get("mismatches", [])
    summary["total_warning"] += len(mismatches)
    for mismatch in mismatches:
        summary["players_warning"].append(
            {
                "name": mismatch.get("name"),
                "player_id": mismatch.get("player_id"),
                "category": category,
                "diffs": mismatch.get("diffs", []),
            },
        )


def _fixed_snapshot_diffs(snapshot: dict[str, Any]) -> list[str]:
    diffs = []
    orig = snapshot.get("original", {})
    calc = snapshot.get("calculated", {})
    for key in _AUTO_REMEDIATION_DIFF_KEYS:
        if key in orig or key in calc:
            o_val = orig.get(key)
            c_val = calc.get(key)
            if o_val != c_val:
                diffs.append(f"{key}: {o_val}→{c_val}")
    return diffs


def _record_auto_remediation_fixed(summary: dict[str, Any], filename: str, content: object) -> None:
    parts = filename.replace(".json", "").split("_")
    category = parts[-1].upper() if len(parts) >= 3 else "UNKNOWN"
    _add_unique_category(summary, "categories_fixed", category)

    snapshots = content if isinstance(content, list) else [content]
    summary["total_fixed"] += len(snapshots)
    for snapshot in snapshots:
        orig = snapshot.get("original", {})
        calc = snapshot.get("calculated", {})
        player_name = snapshot.get("player_name") or calc.get("player_name") or orig.get("player_name")
        summary["players_fixed"].append(
            {
                "name": player_name,
                "player_id": snapshot.get("player_id"),
                "category": category,
                "diffs": _fixed_snapshot_diffs(snapshot),
            },
        )


def _auto_remediation_status(*, has_abort: bool, has_warning: bool, has_fixed: bool) -> str:
    if has_abort:
        return "aborted"
    if has_warning:
        return "warning"
    if has_fixed:
        return "fixed"
    return "no_issues"


def get_auto_remediation_summary(target_date_str: str, audit_dir: Path | None = None) -> dict[str, Any]:
    """
    Scans logs/audit_fixes/ for files starting with target_date_str.

    Parses warning, abort, and fixed player details to return a status summary.
    """
    audit_dir = audit_dir or _default_audit_fix_dir()
    summary = _empty_auto_remediation_summary()

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
            with f.open(encoding="utf-8") as file_handle:
                content = json.load(file_handle)
        except SQLAlchemyError:
            _LOGGER.exception("Failed to read/parse audit fix file %s", filename)
            continue

        if "_abort_" in filename:
            has_abort = True
            _record_auto_remediation_abort(summary, filename, content)
        elif "_warning_" in filename:
            has_warning = True
            _record_auto_remediation_warning(summary, filename, content)
        else:
            has_fixed = True
            _record_auto_remediation_fixed(summary, filename, content)

    summary["status"] = _auto_remediation_status(
        has_abort=has_abort,
        has_warning=has_warning,
        has_fixed=has_fixed,
    )
    return summary


def get_pa_formula_integrity(session: Session, year: int) -> dict[str, Any]:
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
                ),
            ),
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
            ),
        )
        .order_by(Game.game_date, GameBattingStat.game_id),
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


def get_pa_formula_trend(session: Session, months: int = 6) -> dict[str, Any]:
    """Get PA formula violation trend for the last N months."""
    start_date = datetime.now(_KST).date() - timedelta(days=months * 30)

    season_ids = [
        r[0]
        for r in session.execute(
            select(KboSeason.season_id).where(
                or_(
                    KboSeason.league_type_code == 0,
                    KboSeason.league_type_name.in_(_REGULAR_SEASON_NAMES),
                ),
            ),
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
        .order_by(Game.game_date),
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


def get_team_stats_integrity(gate_result: dict[str, Any]) -> dict[str, Any]:
    """
    Gets team stats integrity.

    Args:
        gate_result: Gate Result.

    Returns:
        Dictionary result.

    """
    team_batting = gate_result.get("team_batting", {})
    team_pitching = gate_result.get("team_pitching", {})
    batting_ok = team_batting.get("ok", True)
    pitching_ok = team_pitching.get("ok", True)
    batting_mismatches = team_batting.get("mismatches", [])
    pitching_mismatches = team_pitching.get("mismatches", [])
    return {
        "ok": batting_ok and pitching_ok,
        "batting_ok": batting_ok,
        "pitching_ok": pitching_ok,
        "batting_checked": team_batting.get("checked_players", 0),
        "pitching_checked": team_pitching.get("checked_players", 0),
        "batting_mismatches": batting_mismatches,
        "pitching_mismatches": pitching_mismatches,
        "total_mismatches": len(batting_mismatches) + len(pitching_mismatches),
    }


def get_team_stats_trend(session: Session, gate_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    현재 시즌 team stats 정합성 스냅샷.

    TeamSeason*은 시즌 단위 aggregate라 월별 추세 산출 불가.
    run_quality_gate()를 호출하여 현재 상태만 반환.
    """
    if gate_result is None:
        year = datetime.now(_KST).year
        gate_result = run_quality_gate(session, year)
    integrity = get_team_stats_integrity(gate_result)

    month_key = datetime.now(_KST).strftime("%Y-%m")
    checked = max(integrity["batting_checked"], integrity["pitching_checked"])
    return {
        "months": [
            {
                "month": month_key,
                "batting_violations": len(integrity["batting_mismatches"]),
                "pitching_violations": len(integrity["pitching_mismatches"]),
                "total_violations": integrity["total_mismatches"],
                "teams_checked": checked,
                "ok": integrity["ok"],
            },
        ],
        "direction": "stable",
        "ok": integrity["ok"],
    }


def get_daily_metrics(
    session: Session,
    target_date_str: str,
    gate_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Calculate core collection metrics for a specific date."""
    target_dt = parse_date_str(target_date_str)

    # 1. Game Status Counts
    status_counts = (
        session.query(Game.game_status, func.count(Game.game_id))
        .filter(Game.game_date == target_dt)
        .group_by(Game.game_status)
        .all()
    )
    status_map = dict(status_counts)

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
        from sqlalchemy import create_engine, text

        local_count = session.query(func.count(Game.game_id)).scalar()
        parity_info["local_count"] = local_count

        target_url = get_oci_url()
        if target_url:
            oci_engine = create_engine(target_url)
            with oci_engine.connect() as conn:
                oci_count = conn.execute(text("SELECT count(*) FROM game")).scalar()
                parity_info["oci_count"] = oci_count
                parity_info["diff"] = oci_count - local_count
                parity_info["ok"] = parity_info["diff"] == 0
    except SQLAlchemyError as e:
        _LOGGER.exception("Parity check failed")
        parity_info["ok"] = False
        parity_info["error"] = str(e)

    # 6. Auto-Remediation Summary
    auto_remediation = get_auto_remediation_summary(target_date_str)

    # 7. PA Formula Integrity
    pa_formula_integrity = get_pa_formula_integrity(session, target_dt.year)

    # 8. PA Formula Trend (6-month)
    pa_formula_trend = get_pa_formula_trend(session, months=6)

    # 9. Team Stats Trend (snapshot)
    team_stats_trend = get_team_stats_trend(session, gate_result=gate_result)

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
        "team_stats_trend": team_stats_trend,
    }


def _append_collection_section(lines: list[str], metrics: dict[str, Any]) -> None:
    total = metrics["total_games"]
    comp = metrics["completed_count"]
    status_summary = ", ".join([f"{s}: {c}" for s, c in metrics["status_counts"].items()])
    lines.append(f"📡 <b>Collection</b>: {comp}/{total} games finished")
    lines.append(f"   ({status_summary})")


def _append_parity_section(lines: list[str], parity: dict[str, Any]) -> None:
    if not parity.get("ok", True):
        lines.append(
            f"❓ <b>Parity</b>: Local {parity.get('local_count')} / OCI {parity.get('oci_count')} (Diff: {parity.get('diff')})",
        )


def _append_detail_integrity_section(lines: list[str], metrics: dict[str, Any]) -> None:
    incomplete = [d["game_id"] for d in metrics["detail_integrity"] if not d["is_complete"]]
    if not incomplete:
        lines.append("✅ <b>Integrity</b>: 100% (All details captured)")
    else:
        lines.append(f"⚠️ <b>Integrity</b>: {len(incomplete)} games missing details")
        lines.extend(f"   - {gid}" for gid in incomplete[:3])


def _append_player_stats_section(lines: list[str], gate_result: dict[str, Any]) -> None:
    player_bat_ok = gate_result.get("batting", {}).get("ok", True)
    player_pit_ok = gate_result.get("pitching", {}).get("ok", True)
    if player_bat_ok and player_pit_ok:
        lines.append("✅ <b>Player Stats</b>: Consistent with cumulative totals")
    else:
        bat_miss = len(gate_result["batting"].get("mismatches", []))
        pit_miss = len(gate_result["pitching"].get("mismatches", []))
        total_miss = bat_miss + pit_miss
        lines.append(f"❌ <b>Player Stats</b>: {total_miss} mismatches detected")
        if bat_miss:
            lines.append(f"   - Batting: {bat_miss} issues")
        if pit_miss:
            lines.append(f"   - Pitching: {pit_miss} issues")


def _append_top_performer_section(lines: list[str], metrics: dict[str, Any]) -> None:
    top = metrics.get("top_performer")
    if top:
        lines.append(f"🔥 <b>Top Performer</b>: {top['name']} (WAR: {top['war']})")


def _append_relay_integrity_section(lines: list[str], metrics: dict[str, Any]) -> None:
    relay_integrity = metrics.get("relay_integrity") or {}
    if relay_integrity.get("ok", True):
        lines.append("✅ <b>PBP</b>: Recent/current-season relay complete")
    else:
        recent_count = relay_integrity.get("recent_missing_count", 0)
        season_count = relay_integrity.get("current_season_missing_count", 0)
        lines.append(f"⚠️ <b>PBP</b>: {recent_count} recent / {season_count} current-season games missing")
        lines.extend(f"   - {gid}" for gid in list(relay_integrity.get("missing_game_ids") or [])[:5])


def _append_standings_integrity_section(lines: list[str], metrics: dict[str, Any]) -> None:
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


def _category_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    cat_counts = {}
    for item in items:
        cat = item.get("category", "UNKNOWN").lower()
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    return cat_counts


def _append_auto_remediation_section(lines: list[str], metrics: dict[str, Any]) -> None:
    auto_rem = metrics.get("auto_remediation") or {}
    status = auto_rem.get("status", "no_issues")
    if status == "fixed":
        cat_counts = _category_counts(auto_rem.get("players_fixed", []))
        cat_str = ", ".join([f"{k} {v}" for k, v in cat_counts.items()])
        lines.append(f"🔧 <b>Auto-Remediation</b>: {auto_rem.get('total_fixed')}건 수정 완료 ({cat_str})")
        for p in auto_rem.get("players_fixed", [])[:3]:
            diffs_str = ", ".join(p.get("diffs", [])[:2])
            lines.append(f"   - {p['name']} ({p['category']}): {diffs_str}")
        if len(auto_rem.get("players_fixed", [])) > 3:
            lines.append(f"   - ... 외 {len(auto_rem['players_fixed']) - 3}명")
    elif status == "warning":
        cat_counts = _category_counts(auto_rem.get("players_warning", []))
        cat_str = ", ".join([f"{k} {v}" for k, v in cat_counts.items()])
        lines.append(
            f"⚠️ <b>Auto-Remediation</b>: mismatch {auto_rem.get('total_warning')}건 발견 (수정 비활성화) ({cat_str})",
        )
        for p in auto_rem.get("players_warning", [])[:3]:
            diffs_str = ", ".join(p.get("diffs", [])[:2])
            lines.append(f"   - {p['name']} ({p['category']}): {diffs_str}")
        if len(auto_rem.get("players_warning", [])) > 3:
            lines.append(f"   - ... 외 {len(auto_rem['players_warning']) - 3}명")
    elif status == "aborted":
        cats_str = ", ".join(auto_rem.get("categories_aborted", []))
        lines.append(f"🛑 <b>Auto-Remediation</b>: 작업 중단 ({cats_str})")
        lines.extend(f"   - {r}" for r in auto_rem.get("abort_reasons", [])[:3])
    else:
        lines.append("✅ <b>Auto-Remediation</b>: No issues detected")


def _append_pa_formula_section(lines: list[str], metrics: dict[str, Any]) -> None:
    pa_formula = metrics.get("pa_formula_integrity") or {}
    if pa_formula.get("ok", True):
        lines.append("✅ <b>PA Formula</b>: All consistent (PA=AB+BB+HBP+SH+SF)")
    else:
        count = pa_formula.get("violation_count", 0)
        lines.append(f"❌ <b>PA Formula</b>: {count} violations")
        lines.extend(
            f"   - {v['game_date']} {v['player_name']} PA={v['pa']} ≠ AB+BB+HBP+SH+SF"
            for v in (pa_formula.get("violations") or [])[:3]
        )


def _append_pa_formula_trend_section(lines: list[str], metrics: dict[str, Any]) -> None:
    trend = metrics.get("pa_formula_trend") or {}
    if trend.get("months"):
        direction_icon = (
            "📈" if trend.get("direction") == "worsening" else "📉" if trend.get("direction") == "improving" else "📊"
        )
        lines.append(f"{direction_icon} <b>PA Formula Trend</b> (6mo): {trend['direction']}")
        for m in trend["months"][-4:]:
            icon = "❌" if m["violation_count"] > 0 else "✅"
            lines.append(f"   {icon} {m['month']}: {m['violation_count']}/{m['total_checked']} ({m['violation_pct']}%)")


def _append_team_stats_section(lines: list[str], gate_result: dict[str, Any]) -> None:
    team_stats = get_team_stats_integrity(gate_result)
    if team_stats["ok"]:
        checked = team_stats["batting_checked"] or team_stats["pitching_checked"]
        lines.append(f"🏆 <b>Team Stats</b>: Consistent ({checked} teams checked)")
    else:
        lines.append(f"🏆 <b>Team Stats</b>: {team_stats['total_mismatches']} mismatches")
        for m in team_stats["batting_mismatches"]:
            lines.append(f"   ❌ Batting [{m.get('team_id', '?')}]: {m.get('issue', 'mismatch')}")
            lines.extend(f"      {d}" for d in m.get("diffs", [])[:2])
        for m in team_stats["pitching_mismatches"]:
            lines.append(f"   ❌ Pitching [{m.get('team_id', '?')}]: {m.get('issue', 'mismatch')}")
            lines.extend(f"      {d}" for d in m.get("diffs", [])[:2])


def _append_team_stats_trend_section(lines: list[str], metrics: dict[str, Any]) -> None:
    ts_trend = metrics.get("team_stats_trend") or {}
    if ts_trend.get("months"):
        direction_icon = "📊"
        lines.append(f"{direction_icon} <b>Team Stats Trend</b> (snapshot): {ts_trend['direction']}")
        for m in ts_trend["months"]:
            icon = "❌" if m["total_violations"] > 0 else "✅"
            lines.append(f"   {icon} {m['month']}: {m['total_violations']} violations ({m['teams_checked']} teams)")


def _append_new_players_section(lines: list[str], metrics: dict[str, Any]) -> None:
    if metrics["new_players"]:
        p_names = ", ".join([p["name"] for p in metrics["new_players"][:5]])
        count = len(metrics["new_players"])
        lines.append(f"🆕 <b>New Players</b>: {count} found ({p_names})")


def format_telegram_report(metrics: dict[str, Any], gate_result: dict[str, Any]) -> str:
    """Format the metrics and gate results into a readable Telegram message."""
    parity = metrics.get("parity") or {}
    parity_icon = "✅" if parity.get("ok", True) else "🚨"
    lines = [f"{parity_icon} <b>KBO Quality Report ({metrics['date']})</b>\n"]

    _append_collection_section(lines, metrics)
    _append_parity_section(lines, parity)
    _append_detail_integrity_section(lines, metrics)
    _append_player_stats_section(lines, gate_result)
    _append_top_performer_section(lines, metrics)
    _append_relay_integrity_section(lines, metrics)
    _append_standings_integrity_section(lines, metrics)
    _append_auto_remediation_section(lines, metrics)
    _append_pa_formula_section(lines, metrics)
    _append_pa_formula_trend_section(lines, metrics)
    _append_team_stats_section(lines, gate_result)
    _append_team_stats_trend_section(lines, metrics)
    _append_new_players_section(lines, metrics)

    return "\n".join(lines)


def _has_report_issues(metrics: dict[str, Any], gate_result: dict[str, Any]) -> bool:
    trend = metrics.get("pa_formula_trend") or {}
    team_batting_ok = (gate_result.get("team_batting") or {}).get("ok", True)
    team_pitching_ok = (gate_result.get("team_pitching") or {}).get("ok", True)
    return (
        not gate_result["ok"]
        or any(not d["is_complete"] for d in metrics["detail_integrity"])
        or not (metrics.get("relay_integrity") or {}).get("ok", True)
        or not (metrics.get("standings_integrity") or {}).get("ok", True)
        or metrics.get("auto_remediation", {}).get("status") in ("warning", "aborted")
        or not (metrics.get("pa_formula_integrity") or {}).get("ok", True)
        or trend.get("direction") == "worsening"
        or not team_batting_ok
        or not team_pitching_ok
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Generate KBO Daily Quality Report")
    parser.add_argument("--date", type=str, help="Target date YYYYMMDD (defaults to today)")
    parser.add_argument("--notify", action="store_true", help="Send to Telegram if issues found")
    parser.add_argument("--force-notify", action="store_true", help="Always send to Telegram")
    args = parser.parse_args(argv)

    target_date = args.date or datetime.now(_KST).strftime("%Y%m%d")
    year = int(target_date[:4])

    with SessionLocal() as session:
        metrics = get_daily_metrics(session, target_date)
        gate_result = run_quality_gate(session, year)

    report_json = {"metrics": metrics, "quality_gate": gate_result, "generated_at": datetime.now(_KST).isoformat()}

    # Save to logs
    log_dir = Path("logs/quality_reports")
    log_dir.mkdir(parents=True, exist_ok=True)
    with (log_dir / f"{target_date}.json").open("w", encoding="utf-8") as f:
        json.dump(report_json, f, indent=2, ensure_ascii=False)

    logger.info("✅ Quality report saved to %s/%s.json", log_dir, target_date)

    # Telegram Notification
    telegram_msg = format_telegram_report(metrics, gate_result)

    should_notify = args.force_notify or (args.notify and _has_report_issues(metrics, gate_result))

    if should_notify:
        logger.info("🚀 Sending report to Telegram...")
        SlackWebhookClient.send_alert(telegram_msg)
    else:
        logger.info("%s", "\n" + telegram_msg.replace("<b>", "").replace("</b>", ""))

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
