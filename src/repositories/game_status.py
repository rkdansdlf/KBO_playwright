"""Game status management: update, refresh, and derive game status."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import get_db_session
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.repositories.game_helpers import (
    _canonicalize_game_id,
    _derive_game_status,
    _has_game_child_rows,
)
from src.utils.date_helpers import parse_date_str

logger = logging.getLogger(__name__)


def current_season_year(today: date | None = None) -> int:
    """Return the season year used by automatic runs (KST calendar year)."""
    if today is None:
        today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    return today.year


def game_year_of(game: Game) -> int | None:
    """Return the calendar year of a game's date, or None if unknown."""
    game_date = game.game_date
    if game_date is None:
        return None
    return getattr(game_date, "year", None)


def automatic_write_allowed(game_year: int | None, season_year: int) -> bool:
    """Automatic runs may write only games from the current season.

    Anything else (past seasons, unknown dates) needs an explicit operator run
    with ``allow_historical=True`` (issue #32).
    """
    return game_year is not None and game_year == season_year


def update_game_status(
    game_id: str, status: str, session: Session | None = None, *, allow_historical: bool = False
) -> bool:
    """Update one game's status.

    Guarded: rows outside the current season are refused unless ``allow_historical``
    is True. Explicit operator runs pass game IDs and opt in; automatic scans do not.

    Args:
        game_id: Game ID.
        status: Status.
        session: DB Session.
        allow_historical: Opt in to writes outside the current season.

    """
    if session is None:
        try:
            with get_db_session() as s:
                return update_game_status(game_id, status, session=s, allow_historical=allow_historical)
        except Exception:
            logger.exception("[ERROR] DB Error (Status)")
            return False

    game_id, _ = _canonicalize_game_id(game_id)

    if not game_id or not status:
        return False

    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return False
    if not allow_historical and not automatic_write_allowed(game_year_of(game), current_season_year()):
        logger.warning(
            "[GUARD] Refused automatic status write outside the current season: %s (%s -> %s). "
            "Use allow_historical=True from an explicit operator run.",
            game_id,
            game.game_status,
            status,
        )
        return False
    game.game_status = status
    session.flush()
    return True


def _recompute_game_status(session: Session, game: Game, today: date) -> str:
    """Derive the next status for one game and fill a missing total from inning rows."""
    has_metadata = _has_game_child_rows(session, GameMetadata, game.game_id)
    has_inning = _has_game_child_rows(session, GameInningScore, game.game_id)
    has_lineup = _has_game_child_rows(session, GameLineup, game.game_id)
    has_batting = _has_game_child_rows(session, GameBattingStat, game.game_id)
    has_pitching = _has_game_child_rows(session, GamePitchingStat, game.game_id)
    if game.game_date < today and has_inning and (game.home_score is None or game.away_score is None):
        inning_totals = dict(
            session.query(GameInningScore.team_side, func.sum(GameInningScore.runs))
            .filter(GameInningScore.game_id == game.game_id)
            .group_by(GameInningScore.team_side)
            .all(),
        )
        if inning_totals.get("away") is not None and inning_totals.get("home") is not None:
            game.away_score = int(inning_totals["away"] or 0)
            game.home_score = int(inning_totals["home"] or 0)
    return _derive_game_status(
        game_date=game.game_date,
        home_score=game.home_score,
        away_score=game.away_score,
        current_status=game.game_status,
        has_metadata=has_metadata,
        has_inning_scores=has_inning,
        has_lineups=has_lineup,
        has_batting=has_batting,
        has_pitching=has_pitching,
        today=today,
    )


def refresh_game_status_for_date(
    target_date: str,
    today: date | None = None,
    session: Session | None = None,
) -> dict[str, Any]:
    """Recompute game_status only for one target date (YYYYMMDD).

    Automatic runs write only games from the current season. Rows from other
    seasons are never rewritten; they are counted in ``skipped_past_season``
    (issue #32).

    Args:
        target_date: Target date for the operation.
        today: Today.
        session: DB Session.

    """
    if session is None:
        try:
            with get_db_session() as s:
                return refresh_game_status_for_date(target_date, today, session=s)
        except SQLAlchemyError:
            logger.exception("[ERROR] DB Error (Status Refresh)")
            return {"target_date": target_date, "total": 0, "updated": 0, "status_counts": {}}

    try:
        dt = parse_date_str(target_date)
    except ValueError:
        return {"target_date": target_date, "total": 0, "updated": 0, "status_counts": {}}

    if today is None:
        today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    season_year = current_season_year(today)

    games = session.query(Game).filter(Game.game_date == dt).all()
    status_counts: dict[str, int] = {}
    game_ids: list[str] = []
    updated_game_ids: list[str] = []
    game_ids_by_status: dict[str, list[str]] = {}
    updated = 0
    skipped_past_season = 0
    for game in games:
        game_ids.append(game.game_id)
        if not automatic_write_allowed(game_year_of(game), season_year):
            skipped_past_season += 1
            continue
        next_status = _recompute_game_status(session, game, today)
        status_counts[next_status] = status_counts.get(next_status, 0) + 1
        game_ids_by_status.setdefault(next_status, []).append(game.game_id)
        if game.game_status != next_status:
            game.game_status = next_status
            updated_game_ids.append(game.game_id)
            updated += 1
    session.flush()
    return {
        "target_date": target_date,
        "total": len(games),
        "updated": updated,
        "status_counts": status_counts,
        "skipped_past_season": skipped_past_season,
        "game_ids": sorted(game_ids),
        "updated_game_ids": sorted(updated_game_ids),
        "game_ids_by_status": {status: sorted(ids) for status, ids in game_ids_by_status.items()},
    }
