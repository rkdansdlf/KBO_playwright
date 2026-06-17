"""
Game status management: update, refresh, and derive game status.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
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

logger = logging.getLogger(__name__)


def update_game_status(game_id: str, status: str) -> bool:
    """Update one game's status."""
    game_id, _ = _canonicalize_game_id(game_id)
    if not game_id or not status:
        return False
    with SessionLocal() as session:
        try:
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            if not game:
                return False
            game.game_status = status
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Status)")
            return False
        else:
            return True


def refresh_game_status_for_date(target_date: str, today: date | None = None) -> dict[str, Any]:
    """
    Recompute game_status only for one target date (YYYYMMDD).
    """
    try:
        dt = datetime.strptime(target_date, "%Y%m%d").date()
    except ValueError:
        return {"target_date": target_date, "total": 0, "updated": 0, "status_counts": {}}

    if today is None:
        today = datetime.now(ZoneInfo("Asia/Seoul")).date()
    with SessionLocal() as session:
        try:
            games = session.query(Game).filter(Game.game_date == dt).all()
            status_counts: dict[str, int] = {}
            game_ids: list[str] = []
            updated_game_ids: list[str] = []
            game_ids_by_status: dict[str, list[str]] = {}
            updated = 0
            for game in games:
                game_ids.append(game.game_id)
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
                next_status = _derive_game_status(
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
                status_counts[next_status] = status_counts.get(next_status, 0) + 1
                game_ids_by_status.setdefault(next_status, []).append(game.game_id)
                if game.game_status != next_status:
                    game.game_status = next_status
                    updated_game_ids.append(game.game_id)
                    updated += 1
            session.commit()
            return {
                "target_date": target_date,
                "total": len(games),
                "updated": updated,
                "status_counts": status_counts,
                "game_ids": sorted(game_ids),
                "updated_game_ids": sorted(updated_game_ids),
                "game_ids_by_status": {status: sorted(ids) for status, ids in game_ids_by_status.items()},
            }
        except SQLAlchemyError:
            session.rollback()
            logger.exception("[ERROR] DB Error (Status Refresh)")
            return {"target_date": target_date, "total": 0, "updated": 0, "status_counts": {}}
