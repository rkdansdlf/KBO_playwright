"""FastAPI router for KBO Futures League schedule and results."""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select

from src.api.auth import get_api_key
from src.db.engine import get_db_session
from src.models.futures_schedule import FuturesGameSchedule

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/futures", tags=["KBO Futures League"])


@router.get("/schedule", dependencies=[Depends(get_api_key)])
def get_futures_schedule(
    season: Annotated[int, Query(description="시즌 연도")] = 2026,
    month: Annotated[int | None, Query(ge=1, le=12, description="월 필터 (1~12)")] = None,
    team_code: Annotated[str | None, Query(description="구단 코드")] = None,
    limit: Annotated[int, Query(ge=1, le=500, description="최대 반환 수")] = 100,
) -> dict[str, Any]:
    """KBO 퓨처스 리그 일정 및 경기 결과를 조회합니다."""
    with get_db_session() as session:
        stmt = select(FuturesGameSchedule).where(FuturesGameSchedule.season == season)

        if team_code:
            stmt = stmt.where(
                (FuturesGameSchedule.away_team == team_code) | (FuturesGameSchedule.home_team == team_code)
            )

        stmt = stmt.order_by(FuturesGameSchedule.game_date.asc()).limit(limit)
        records = list(session.execute(stmt).scalars().all())

        if month is not None:
            records = [r for r in records if r.game_date and r.game_date.month == month]

        results = [
            {
                "game_id": r.game_id,
                "season": r.season,
                "game_date": r.game_date.isoformat() if r.game_date else None,
                "away_team": r.away_team,
                "home_team": r.home_team,
                "away_score": r.away_score,
                "home_score": r.home_score,
                "stadium": r.stadium,
                "game_status": r.game_status,
                "cancel_reason": r.cancel_reason,
            }
            for r in records
        ]

        return {
            "season": season,
            "count": len(results),
            "schedules": results,
        }
