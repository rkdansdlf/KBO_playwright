"""P0 game-data readiness checks.

This module keeps the operational readiness rules in one place so the CLI,
daily finalize summary, and freshness monitor report the same gaps.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.constants import DATE_STR_LEN
from src.models.broadcast import GameBroadcast
from src.models.game import (
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)
from src.models.roster_transaction import RosterTransaction
from src.models.team import TeamDailyRoster
from src.utils.game_status import (
    COMPLETED_LIKE_GAME_STATUSES,
    GAME_STATUS_CANCELLED,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    is_live_status,
)

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")
FALSE_ENV_VALUES = {"0", "false", "no", "off"}
P0_READINESS_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_ENV_VALUES


def normalize_yyyymmdd(value: str | date | datetime | None) -> str:
    if value is None:
        return datetime.now(KST).strftime("%Y%m%d")
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    normalized = str(value).replace("-", "").strip()
    if len(normalized) != DATE_STR_LEN or not normalized.isdigit():
        msg = f"Invalid date: {value!r}. Use YYYYMMDD."
        raise ValueError(msg)
    datetime.strptime(normalized, "%Y%m%d")
    return normalized


def _date_from_yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _date_key(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return str(value or "").replace("-", "")[:8]


def _status(value: object) -> str:
    return str(value or "").upper().strip()


def _is_cancelled_or_postponed(game: object) -> bool:
    return _status(game.game_status) in {GAME_STATUS_CANCELLED, GAME_STATUS_POSTPONED}


def _has_text(value: object) -> bool:
    return bool(str(value or "").strip())


def _safe_rows(query: object) -> list[Any]:
    try:
        return list(query.all())
    except P0_READINESS_DB_EXCEPTIONS:
        logger.exception("P0 readiness query failed")
        return []


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(raw_value[:10] if fmt == "%Y-%m-%d" else raw_value[:8], fmt).date()
        except ValueError:
            continue
    return None


def _query_games(session: Session, start: date, end: date) -> list[Any]:
    required = (
        "game_id",
        "game_date",
        "stadium",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "away_pitcher",
        "home_pitcher",
        "game_status",
        "game_lifecycle_state",
    )
    try:
        existing_columns = {column["name"] for column in inspect(session.get_bind()).get_columns("game")}
    except P0_READINESS_DB_EXCEPTIONS:
        logger.exception("P0 readiness could not inspect game table columns")
        existing_columns = set(required)

    if "game_id" not in existing_columns or "game_date" not in existing_columns:
        logger.warning("P0 readiness requires game.game_id and game.game_date columns")
        return []

    select_columns = [column if column in existing_columns else f"NULL AS {column}" for column in required]
    try:
        rows = (
            session.execute(
                text(
                    f"""
                    SELECT {", ".join(select_columns)}
                    FROM game
                    WHERE game_date BETWEEN :start_date AND :end_date
                    ORDER BY game_date, game_id
                    """,  # noqa: S608
                ),
                {"start_date": start, "end_date": end},
            )
            .mappings()
            .all()
        )
    except P0_READINESS_DB_EXCEPTIONS:
        logger.exception("P0 readiness game query failed")
        return []
    games: list[Any] = []
    for row in rows:
        payload = dict(row)
        payload["game_date"] = _coerce_date(payload.get("game_date"))
        games.append(SimpleNamespace(**payload))
    return games


def _metadata_by_game(session: Session, game_ids: Iterable[str]) -> dict[str, GameMetadata]:
    ids = [gid for gid in game_ids if gid]
    if not ids:
        return {}
    rows = _safe_rows(session.query(GameMetadata).filter(GameMetadata.game_id.in_(ids)))
    return {row.game_id: row for row in rows}


def _count_by_game(session: Session, model: type[Any], game_ids: Iterable[str]) -> dict[str, int]:
    ids = [gid for gid in game_ids if gid]
    if not ids:
        return {}
    rows = _safe_rows(session.query(model.game_id, func.count()).filter(model.game_id.in_(ids)).group_by(model.game_id))
    return {str(game_id): int(count or 0) for game_id, count in rows}


def _lineup_sides_by_game(session: Session, game_ids: Iterable[str]) -> dict[str, set[str]]:
    ids = [gid for gid in game_ids if gid]
    if not ids:
        return {}
    rows = _safe_rows(
        session.query(GameLineup.game_id, GameLineup.team_side)
        .filter(GameLineup.game_id.in_(ids), GameLineup.is_starter.is_(True))
        .group_by(GameLineup.game_id, GameLineup.team_side),
    )
    sides: dict[str, set[str]] = {}
    for game_id, team_side in rows:
        if team_side:
            sides.setdefault(str(game_id), set()).add(str(team_side))
    return sides


def _side_counts_by_game(session: Session, model: type[Any], game_ids: Iterable[str]) -> dict[str, set[str]]:
    ids = [gid for gid in game_ids if gid]
    if not ids:
        return {}
    rows = _safe_rows(
        session.query(model.game_id, model.team_side)
        .filter(model.game_id.in_(ids))
        .group_by(model.game_id, model.team_side),
    )
    sides: dict[str, set[str]] = {}
    for game_id, team_side in rows:
        if team_side:
            sides.setdefault(str(game_id), set()).add(str(team_side))
    return sides


def _preview_games(session: Session, game_ids: Iterable[str]) -> set[str]:
    ids = [gid for gid in game_ids if gid]
    if not ids:
        return set()
    rows = _safe_rows(
        session.query(GameSummary.game_id)
        .filter(GameSummary.game_id.in_(ids), GameSummary.summary_type == "프리뷰")
        .group_by(GameSummary.game_id),
    )
    return {str(row[0]) for row in rows}


def _pitching_decision_games(session: Session, game_ids: Iterable[str]) -> set[str]:
    ids = [gid for gid in game_ids if gid]
    if not ids:
        return set()
    rows = _safe_rows(
        session.query(GamePitchingStat.game_id)
        .filter(
            GamePitchingStat.game_id.in_(ids),
            (
                (GamePitchingStat.wins > 0)
                | (GamePitchingStat.losses > 0)
                | (GamePitchingStat.saves > 0)
                | (GamePitchingStat.holds > 0)
                | GamePitchingStat.decision.is_not(None)
            ),
        )
        .group_by(GamePitchingStat.game_id),
    )
    return {str(row[0]) for row in rows}


def _rows_by_date(session: Session, _model: type[Any], date_column: object, dates: Iterable[date]) -> dict[str, int]:
    date_list = list(dates)
    if not date_list:
        return {}
    rows = _safe_rows(session.query(date_column, func.count()).filter(date_column.in_(date_list)).group_by(date_column))
    return {_date_key(row_date): int(count or 0) for row_date, count in rows}


def _add_failure(
    failures: list[dict[str, Any]],
    *,
    dataset: str,
    reason: str,
    game_id: str | None = None,
    game_date: str | None = None,
    severity: str = "warning",
) -> None:
    failures.append(
        {
            "dataset": dataset,
            "game_id": game_id,
            "game_date": game_date,
            "reason": reason,
            "severity": severity,
        },
    )


def _coverage(ok: int, total: int) -> float:
    return 100.0 if total == 0 else round((ok / total) * 100, 1)


def _score_present(game: object) -> bool:
    return game.home_score is not None and game.away_score is not None


def _meta_has_start_time(meta: GameMetadata | None) -> bool:
    return bool(meta and meta.start_time is not None)


def _meta_has_stadium(game: object, meta: GameMetadata | None) -> bool:
    return _has_text(getattr(game, "stadium", None)) or bool(
        meta and (_has_text(meta.stadium_name) or _has_text(meta.stadium_code)),
    )


def _broadcast_missing_reason(game: object, target_day: date) -> str:
    game_day = getattr(game, "game_date", None)
    if _status(getattr(game, "game_status", None)) == GAME_STATUS_SCHEDULED or (
        isinstance(game_day, date) and game_day > target_day
    ):
        return "broadcast_not_announced"
    return "broadcast_source_unavailable"


def _partition_games(games: list[Any], target_day: date) -> dict[str, Any]:
    active_games = [game for game in games if not _is_cancelled_or_postponed(game)]
    scheduled_games = [
        game
        for game in active_games
        if _status(game.game_status) == GAME_STATUS_SCHEDULED and game.game_date and game.game_date >= target_day
    ]
    live_games = [
        game
        for game in active_games
        if is_live_status(game.game_status)
        or str(game.game_lifecycle_state or "").lower() in {"running", "delayed", "suspended"}
    ]
    completed_games = [game for game in active_games if _status(game.game_status) in COMPLETED_LIKE_GAME_STATUSES]
    return {
        "active_games": active_games,
        "scheduled_games": scheduled_games,
        "live_games": live_games,
        "completed_games": completed_games,
        "relay_games": sorted({game.game_id for game in completed_games + live_games if game.game_id}),
    }


def _check_schedule_completeness(
    active_games: list[Any],
    metadata: dict[str, GameMetadata],
    failures: list[dict[str, Any]],
) -> None:
    for game in active_games:
        game_date = _date_key(game.game_date)
        meta = metadata.get(game.game_id)
        if not _has_text(game.home_team) or not _has_text(game.away_team):
            _add_failure(
                failures,
                dataset="schedule",
                game_id=game.game_id,
                game_date=game_date,
                reason="missing_team",
                severity="critical",
            )
        if not _has_text(game.game_status):
            _add_failure(
                failures,
                dataset="schedule",
                game_id=game.game_id,
                game_date=game_date,
                reason="missing_status",
                severity="critical",
            )
        if not _meta_has_start_time(meta):
            _add_failure(
                failures, dataset="schedule", game_id=game.game_id, game_date=game_date, reason="missing_start_time"
            )
        if not _meta_has_stadium(game, meta):
            _add_failure(
                failures, dataset="schedule", game_id=game.game_id, game_date=game_date, reason="missing_stadium"
            )


def _check_pregame_completeness(
    session: Session,
    scheduled_games: list[Any],
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    lineup_sides = _lineup_sides_by_game(session, [game.game_id for game in scheduled_games])
    preview_ids = _preview_games(session, [game.game_id for game in scheduled_games])
    starters_complete = 0
    lineups_complete = 0

    for game in scheduled_games:
        game_date = _date_key(game.game_date)
        if _has_text(game.away_pitcher) and _has_text(game.home_pitcher):
            starters_complete += 1
        else:
            _add_failure(
                failures, dataset="pregame", game_id=game.game_id, game_date=game_date, reason="missing_starter"
            )

        if {"away", "home"} <= lineup_sides.get(game.game_id, set()):
            lineups_complete += 1
        else:
            _add_failure(
                failures, dataset="pregame", game_id=game.game_id, game_date=game_date, reason="missing_lineup"
            )

        if game.game_id not in preview_ids:
            _add_failure(
                failures, dataset="pregame", game_id=game.game_id, game_date=game_date, reason="missing_preview"
            )

    return {"starters_complete": starters_complete, "lineups_complete": lineups_complete, "preview_ids": preview_ids}


def _max_innings_by_game(session: Session, relay_games: list[str]) -> dict[str, int]:
    if not relay_games:
        return {}
    rows = _safe_rows(
        session.query(GamePlayByPlay.game_id, func.max(GamePlayByPlay.inning))
        .filter(GamePlayByPlay.game_id.in_(relay_games))
        .group_by(GamePlayByPlay.game_id),
    )
    return {str(game_id): int(max_inning or 0) for game_id, max_inning in rows}


def _check_live_completeness(
    live_games: list[Any],
    event_counts: dict[str, int],
    pbp_counts: dict[str, int],
    failures: list[dict[str, Any]],
) -> None:
    for game in live_games:
        game_date = _date_key(game.game_date)
        has_relay = event_counts.get(game.game_id, 0) > 0 or pbp_counts.get(game.game_id, 0) > 0
        if not has_relay:
            _add_failure(
                failures, dataset="live", game_id=game.game_id, game_date=game_date, reason="missing_live_relay"
            )
        if not _score_present(game):
            _add_failure(
                failures, dataset="live", game_id=game.game_id, game_date=game_date, reason="missing_live_score"
            )


def _check_postgame_completeness(
    session: Session,
    completed_games: list[Any],
    failures: list[dict[str, Any]],
) -> dict[str, int]:
    inning_counts = _count_by_game(session, GameInningScore, [game.game_id for game in completed_games])
    batting_sides = _side_counts_by_game(session, GameBattingStat, [game.game_id for game in completed_games])
    pitching_sides = _side_counts_by_game(session, GamePitchingStat, [game.game_id for game in completed_games])
    decision_ids = _pitching_decision_games(session, [game.game_id for game in completed_games])
    counts = {"scores_ok": 0, "details_ok": 0, "innings_ok": 0, "decisions_ok": 0}

    for game in completed_games:
        game_date = _date_key(game.game_date)
        if _score_present(game):
            counts["scores_ok"] += 1
        else:
            _add_failure(
                failures,
                dataset="postgame",
                game_id=game.game_id,
                game_date=game_date,
                reason="missing_final_score",
                severity="critical",
            )

        has_batting = {"away", "home"} <= batting_sides.get(game.game_id, set())
        has_pitching = {"away", "home"} <= pitching_sides.get(game.game_id, set())
        if has_batting and has_pitching:
            counts["details_ok"] += 1
        else:
            _add_failure(
                failures,
                dataset="postgame",
                game_id=game.game_id,
                game_date=game_date,
                reason="missing_boxscore_detail",
                severity="critical",
            )

        if inning_counts.get(game.game_id, 0) > 0:
            counts["innings_ok"] += 1
        else:
            _add_failure(
                failures, dataset="postgame", game_id=game.game_id, game_date=game_date, reason="missing_inning_score"
            )

        if game.game_id in decision_ids:
            counts["decisions_ok"] += 1
        else:
            _add_failure(
                failures,
                dataset="postgame",
                game_id=game.game_id,
                game_date=game_date,
                reason="missing_pitcher_decision",
            )

    return counts


def _check_relay_completeness(
    relay_games: list[str],
    active_games: list[Any],
    event_counts: dict[str, int],
    pbp_counts: dict[str, int],
    failures: list[dict[str, Any]],
) -> int:
    relay_ok = 0
    active_by_game_id = {game.game_id: game for game in active_games}
    for game_id in relay_games:
        game = active_by_game_id.get(game_id)
        game_date = _date_key(game.game_date) if game else None
        if event_counts.get(game_id, 0) > 0 or pbp_counts.get(game_id, 0) > 0:
            relay_ok += 1
        else:
            severity = "critical" if game and _status(game.game_status) in COMPLETED_LIKE_GAME_STATUSES else "warning"
            _add_failure(
                failures,
                dataset="relay",
                game_id=game_id,
                game_date=game_date,
                reason="missing_relay",
                severity=severity,
            )
    return relay_ok


def _check_roster_completeness(
    session: Session,
    active_games: list[Any],
    target_day: date,
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    roster_dates = sorted({game.game_date for game in active_games if game.game_date and game.game_date <= target_day})
    daily_roster_rows = _rows_by_date(session, TeamDailyRoster, TeamDailyRoster.roster_date, roster_dates)
    transaction_rows = _rows_by_date(session, RosterTransaction, RosterTransaction.transaction_date, roster_dates)
    roster_dates_ok = 0
    for roster_date in roster_dates:
        key = _date_key(roster_date)
        if daily_roster_rows.get(key, 0) > 0:
            roster_dates_ok += 1
        else:
            _add_failure(failures, dataset="roster", game_date=key, reason="missing_daily_roster")
    return {
        "roster_dates": roster_dates,
        "daily_roster_rows": daily_roster_rows,
        "transaction_rows": transaction_rows,
        "roster_dates_ok": roster_dates_ok,
    }


def _check_broadcast_completeness(
    session: Session,
    active_games: list[Any],
    target_day: date,
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    broadcast_counts = _count_by_game(session, GameBroadcast, [game.game_id for game in active_games])
    broadcast_ok = 0
    skip_counts: dict[str, int] = {}
    skip_game_ids: dict[str, list[str]] = {}
    for game in active_games:
        if broadcast_counts.get(game.game_id, 0) > 0:
            broadcast_ok += 1
        else:
            reason = _broadcast_missing_reason(game, target_day)
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            skip_game_ids.setdefault(reason, []).append(str(game.game_id))
            _add_failure(
                failures, dataset="broadcast", game_id=game.game_id, game_date=_date_key(game.game_date), reason=reason
            )
    return {"broadcast_ok": broadcast_ok, "skip_counts": skip_counts, "skip_game_ids": skip_game_ids}


def build_p0_readiness(
    session: Session,
    *,
    target_date: str | date | datetime | None = None,
    lookback_days: int = 7,
    lookahead_days: int = 1,
    oci_skip_counts: dict[str, int] | None = None,
    oci_skip_game_ids: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a JSON-serializable P0 readiness report for a date window."""
    target = normalize_yyyymmdd(target_date)
    target_day = _date_from_yyyymmdd(target)
    start_day = target_day - timedelta(days=max(0, int(lookback_days or 0)))
    end_day = target_day + timedelta(days=max(0, int(lookahead_days or 0)))

    games = _query_games(session, start_day, end_day)
    game_ids = [game.game_id for game in games if game.game_id]
    metadata = _metadata_by_game(session, game_ids)
    failures: list[dict[str, Any]] = []

    partitions = _partition_games(games, target_day)
    active_games = partitions["active_games"]
    scheduled_games = partitions["scheduled_games"]
    live_games = partitions["live_games"]
    completed_games = partitions["completed_games"]
    relay_games = partitions["relay_games"]

    _check_schedule_completeness(active_games, metadata, failures)
    pregame = _check_pregame_completeness(session, scheduled_games, failures)

    event_counts = _count_by_game(session, GameEvent, relay_games)
    pbp_counts = _count_by_game(session, GamePlayByPlay, relay_games)
    max_innings = _max_innings_by_game(session, relay_games)
    _check_live_completeness(live_games, event_counts, pbp_counts, failures)
    postgame = _check_postgame_completeness(session, completed_games, failures)
    relay_ok = _check_relay_completeness(relay_games, active_games, event_counts, pbp_counts, failures)
    roster = _check_roster_completeness(session, active_games, target_day, failures)
    broadcast = _check_broadcast_completeness(session, active_games, target_day, failures)

    pregame_total = len(scheduled_games)
    postgame_total = len(completed_games)
    live_total = len(live_games)
    relay_total = len(relay_games)
    roster_total = len(roster["roster_dates"])
    broadcast_total = len(active_games)
    critical_failure_count = sum(1 for failure in failures if failure.get("severity") == "critical")

    oci_sync_enabled = _env_enabled("P0_OCI_READINESS_EXPECT_SYNC", "0") or bool(os.getenv("OCI_DB_URL"))
    oci_target_present = bool(os.getenv("OCI_DB_URL"))
    if oci_sync_enabled and not oci_target_present:
        _add_failure(failures, dataset="oci", reason="sync_not_ready")

    return {
        "generated_at": datetime.now(KST).isoformat(),
        "target_date": target,
        "start_date": start_day.strftime("%Y%m%d"),
        "end_date": end_day.strftime("%Y%m%d"),
        "window": {
            "lookback_days": max(0, int(lookback_days or 0)),
            "lookahead_days": max(0, int(lookahead_days or 0)),
        },
        "schedule": {
            "games": len(games),
            "active_games": len(active_games),
            "cancelled_or_postponed": len(games) - len(active_games),
            "with_start_time": sum(1 for game in active_games if _meta_has_start_time(metadata.get(game.game_id))),
            "with_stadium": sum(1 for game in active_games if _meta_has_stadium(game, metadata.get(game.game_id))),
        },
        "pregame": {
            "games": pregame_total,
            "starters_complete": pregame["starters_complete"],
            "lineups_complete": pregame["lineups_complete"],
            "preview_rows": len(pregame["preview_ids"]),
            "starter_coverage_pct": _coverage(pregame["starters_complete"], pregame_total),
            "lineup_coverage_pct": _coverage(pregame["lineups_complete"], pregame_total),
        },
        "live": {
            "games": live_total,
            "with_relay": sum(
                1 for game in live_games if event_counts.get(game.game_id, 0) > 0 or pbp_counts.get(game.game_id, 0) > 0
            ),
            "with_score": sum(1 for game in live_games if _score_present(game)),
            "max_inning_by_game": {game.game_id: max_innings.get(game.game_id, 0) for game in live_games},
        },
        "postgame": {
            "games": postgame_total,
            "scores_complete": postgame["scores_ok"],
            "boxscore_detail_complete": postgame["details_ok"],
            "inning_scores_present": postgame["innings_ok"],
            "pitcher_decisions_present": postgame["decisions_ok"],
            "score_coverage_pct": _coverage(postgame["scores_ok"], postgame_total),
            "detail_coverage_pct": _coverage(postgame["details_ok"], postgame_total),
        },
        "relay": {
            "games": relay_total,
            "with_events_or_pbp": relay_ok,
            "event_rows": sum(event_counts.values()),
            "pbp_rows": sum(pbp_counts.values()),
            "coverage_pct": _coverage(relay_ok, relay_total),
        },
        "roster": {
            "dates": roster_total,
            "daily_roster_dates": roster["roster_dates_ok"],
            "daily_roster_rows": sum(roster["daily_roster_rows"].values()),
            "transaction_rows": sum(roster["transaction_rows"].values()),
            "coverage_pct": _coverage(roster["roster_dates_ok"], roster_total),
        },
        "broadcast": {
            "games": broadcast_total,
            "with_broadcast": broadcast["broadcast_ok"],
            "missing": broadcast_total - broadcast["broadcast_ok"],
            "coverage_pct": _coverage(broadcast["broadcast_ok"], broadcast_total),
            "skip_counts": dict(sorted(broadcast["skip_counts"].items())),
            "skip_game_ids": {key: sorted(set(value)) for key, value in broadcast["skip_game_ids"].items()},
        },
        "oci": {
            "sync_enabled": oci_sync_enabled,
            "target_url_present": oci_target_present,
            "skip_counts": dict(sorted((oci_skip_counts or {}).items())),
            "skip_game_ids": {key: sorted(set(value)) for key, value in (oci_skip_game_ids or {}).items()},
        },
        "failures": failures,
        "summary": {
            "ok": critical_failure_count == 0,
            "failure_count": len(failures),
            "critical_failure_count": critical_failure_count,
            "warning_count": len(failures) - critical_failure_count,
        },
    }


def format_p0_readiness_summary(readiness: dict[str, Any] | None) -> str:
    if not isinstance(readiness, dict):
        return "p0=unavailable"
    summary = readiness.get("summary") if isinstance(readiness.get("summary"), dict) else {}
    schedule = readiness.get("schedule") if isinstance(readiness.get("schedule"), dict) else {}
    pregame = readiness.get("pregame") if isinstance(readiness.get("pregame"), dict) else {}
    postgame = readiness.get("postgame") if isinstance(readiness.get("postgame"), dict) else {}
    relay = readiness.get("relay") if isinstance(readiness.get("relay"), dict) else {}
    return (
        f"p0_ok={summary.get('ok', False)} "
        f"p0_failures={summary.get('failure_count', 0)} "
        f"p0_critical={summary.get('critical_failure_count', 0)} "
        f"schedule_games={schedule.get('games', 0)} "
        f"pregame={pregame.get('starters_complete', 0)}/{pregame.get('games', 0)} "
        f"postgame={postgame.get('boxscore_detail_complete', 0)}/{postgame.get('games', 0)} "
        f"relay={relay.get('with_events_or_pbp', 0)}/{relay.get('games', 0)}"
    )
