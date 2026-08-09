"""Health check and system status router."""

from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.api.auth import get_api_key
from src.db.engine import get_db_session
from src.models.game import Game
from src.models.player import PlayerBasic, PlayerMovement
from src.utils.lock import ProcessLock
from src.utils.metrics import record_api_cache

logger = logging.getLogger(__name__)

_status_state: dict[str, Any] = {"data": None, "ts": 0.0}
_STATUS_CACHE_TTL_SECONDS = 30

_healing_state: dict[str, Any] = {"data": None, "ts": 0.0}
_HEALING_CACHE_TTL_SECONDS = 60

router = APIRouter(tags=["Health & Status"])


def _check_lock_status(lock_name: str) -> bool:
    """Check if a ProcessLock is currently held by attempting a non-blocking acquire."""
    lock = ProcessLock(lock_name, blocking=False)
    if lock.acquire():
        lock.release()
        return False
    return True


def _build_status_payload() -> dict[str, Any]:
    """Query database statistics and lock statuses, then update the status cache."""
    with get_db_session() as session:
        game_count = session.query(Game).count()
        player_count = session.query(PlayerBasic).count()
        latest_game = session.query(Game).order_by(Game.game_date.desc()).first()
        latest_game_date = str(latest_game.game_date) if latest_game else None

        latest_movement = session.query(PlayerMovement).order_by(PlayerMovement.created_at.desc()).first()
        latest_movement_at = latest_movement.created_at.isoformat() if latest_movement else None

    payload = {
        "database": {
            "games_count": game_count,
            "players_count": player_count,
            "latest_game_date": latest_game_date,
            "latest_roster_movement_at": latest_movement_at,
        },
        "locks": {
            "live_refresh": _check_lock_status("live_refresh"),
            "daily_update": _check_lock_status("daily_update"),
            "maintenance": _check_lock_status("maintenance"),
        },
    }
    _status_state["data"] = payload
    _status_state["ts"] = time.monotonic()
    return payload


def _build_healing_payload() -> dict[str, Any]:
    """Query DB integrity status, then update the healing cache."""
    from src.cli.auto_healer import (
        _find_inconsistent_games,
        _find_pa_formula_inconsistent_games,
        _find_season_stat_discrepancies,
        _find_stuck_games,
    )

    stuck_games = _find_stuck_games()
    inconsistent = _find_inconsistent_games()
    pa_inconsistent = _find_pa_formula_inconsistent_games()
    discrepant_seasons = _find_season_stat_discrepancies()

    payload = {
        "status": (
            "clean" if not (stuck_games or inconsistent or pa_inconsistent or discrepant_seasons) else "action_required"
        ),
        "stuck_games_count": len(stuck_games),
        "score_inconsistent_count": len(inconsistent),
        "pa_formula_inconsistent_count": len(pa_inconsistent),
        "discrepant_seasons": discrepant_seasons,
    }
    _healing_state["data"] = payload
    _healing_state["ts"] = time.monotonic()
    return payload


@router.get("/health")
def health_check() -> dict[str, str]:
    """Provide a simple health check endpoint."""
    return {"status": "ok"}


@router.get("/metrics")
def get_prometheus_metrics() -> Response:
    """Prometheus metrics scrape endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/status", dependencies=[Depends(get_api_key)])
def get_system_status() -> dict[str, Any]:
    """Query database statistics and system lock statuses."""
    if _status_state["data"] is not None and (
        time.monotonic() - _status_state["ts"]
    ) < _STATUS_CACHE_TTL_SECONDS:
        record_api_cache("/status", hit=True)
        return _status_state["data"]
    record_api_cache("/status", hit=False)
    try:
        return _build_status_payload()
    except Exception as e:
        logger.exception("Failed to query system status")
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}") from e


@router.get("/api/v1/health/healing-status", dependencies=[Depends(get_api_key)])
def get_healing_status() -> dict[str, Any]:
    """DB 무결성, 멈춘 경기 수, 시즌 통계 불일치 여부를 리포트합니다."""
    if _healing_state["data"] is not None and (
        time.monotonic() - _healing_state["ts"]
    ) < _HEALING_CACHE_TTL_SECONDS:
        record_api_cache("/healing-status", hit=True)
        return _healing_state["data"]
    record_api_cache("/healing-status", hit=False)
    try:
        return _build_healing_payload()
    except Exception as e:
        logger.exception("Failed to query healing status")
        raise HTTPException(status_code=500, detail=str(e)) from e
