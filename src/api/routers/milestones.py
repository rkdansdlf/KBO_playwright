"""FastAPI router for KBO player milestone achievements and countdowns."""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select

from src.api.auth import get_api_key
from src.api.schemas import MilestonesListResponse
from src.db.engine import get_db_session
from src.models.player_milestone import PlayerMilestone

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/milestones", tags=["KBO Player Milestones"])


@router.get(
    "",
    dependencies=[Depends(get_api_key)],
    response_model=MilestonesListResponse,
    summary="KBO 선수 통산 대기록 현황 및 달성 임박 조회",
)
def get_milestones(
    season: Annotated[int, Query(description="시즌 연도")] = 2026,
    team_code: Annotated[str | None, Query(description="구단 코드 (LG, KIA 등)")] = None,
    is_achieved: Annotated[bool | None, Query(description="달성 여부 필터")] = None,
    limit: Annotated[int, Query(ge=1, le=200, description="최대 반환 항목 수")] = 50,
) -> dict[str, Any]:
    """KBO 선수들의 안타, 홈런, 타점, 승수 등 통산 대기록 달성 현황 및 달성 임박 카운트다운을 조회합니다."""
    with get_db_session() as session:
        stmt = select(PlayerMilestone).where(PlayerMilestone.season == season)

        if team_code:
            stmt = stmt.where(PlayerMilestone.team_code == team_code)
        if is_achieved is not None:
            stmt = stmt.where(PlayerMilestone.is_achieved == is_achieved)

        stmt = stmt.order_by(PlayerMilestone.remaining_val.asc()).limit(limit)

        records = list(session.execute(stmt).scalars().all())
        results = [
            {
                "id": r.id,
                "season": r.season,
                "player_id": r.player_id,
                "player_name": r.player_name,
                "team_code": r.team_code,
                "milestone_category": r.milestone_category,
                "current_val": r.current_val,
                "target_val": r.target_val,
                "remaining_val": r.remaining_val,
                "is_achieved": r.is_achieved,
                "achieved_date": r.achieved_date.isoformat() if r.achieved_date else None,
            }
            for r in records
        ]

        return {
            "season": season,
            "count": len(results),
            "milestones": results,
        }
