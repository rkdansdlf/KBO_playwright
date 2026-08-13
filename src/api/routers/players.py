"""Players and Teams API Router."""

from __future__ import annotations

import logging
import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from src.api.auth import get_api_key
from src.api.schemas import PlayerSplitsResponse
from src.db.engine import get_db_session
from src.models.player import PlayerBasic
from src.models.player_draft import PlayerDraftHistory
from src.models.player_splits_stat import PlayerSplitsStat
from src.models.team import Team

logger = logging.getLogger(__name__)

_teams_state: dict[str, Any] = {"data": None, "ts": 0.0}
_TEAMS_CACHE_TTL_SECONDS = 3600  # 1 hour

router = APIRouter(tags=["Players & Teams"])


def _build_teams_payload() -> dict[str, Any]:
    """Query the full team list and update the teams cache."""
    with get_db_session() as session:
        teams = session.query(Team).order_by(Team.team_id).all()
        results = [
            {
                "team_id": t.team_id,
                "team_name": t.team_name,
                "team_short_name": t.team_short_name,
                "city": t.city,
                "founded_year": t.founded_year,
                "stadium_name": t.stadium_name,
                "is_active": t.is_active,
            }
            for t in teams
        ]
    payload = {"teams": results}
    _teams_state["data"] = payload
    _teams_state["ts"] = time.monotonic()
    return payload


@router.get(
    "/api/players",
    dependencies=[Depends(get_api_key)],
    summary="KBO 선수 프로필 목록 검색",
)
@router.get(
    "/api/v1/players",
    dependencies=[Depends(get_api_key)],
    summary="KBO 선수 프로필 목록 검색 (v1)",
)
def get_players(
    name: Annotated[str | None, Query(description="선수 이름 검색 (부분 일치)")] = None,
    team: Annotated[str | None, Query(description="소속 팀 이름 또는 코드")] = None,
    position: Annotated[str | None, Query(description="포지션 (예: 투수, 포수, 내야수, 외야수)")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    """Query KBO player basic profile list with optional filters."""
    try:
        with get_db_session() as session:
            query = session.query(PlayerBasic)
            if name:
                query = query.filter(PlayerBasic.name.contains(name))
            if team:
                query = query.filter(PlayerBasic.team.contains(team))
            if position:
                query = query.filter(PlayerBasic.position.contains(position))

            total_count = query.count()
            players = query.order_by(PlayerBasic.player_id).offset(offset).limit(limit).all()

            results = [
                {
                    "player_id": p.player_id,
                    "name": p.name,
                    "uniform_no": p.uniform_no,
                    "team": p.team,
                    "position": p.position,
                    "birth_date": p.birth_date,
                    "height_cm": p.height_cm,
                    "weight_kg": p.weight_kg,
                    "career": p.career,
                    "status": p.status,
                }
                for p in players
            ]
            return {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "players": results,
            }
    except Exception as e:
        logger.exception("Failed to query players")
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}") from e


@router.get(
    "/api/v1/players/drafts",
    dependencies=[Depends(get_api_key)],
    summary="KBO 신인 드래프트 지명 이력 조회",
)
def get_player_drafts(
    season: Annotated[int, Query(description="지명 시즌 연도")] = 2026,
    team_code: Annotated[str | None, Query(description="구단 코드")] = None,
) -> dict[str, Any]:
    """Query KBO rookie draft history entries."""
    with get_db_session() as session:
        stmt = select(PlayerDraftHistory).where(PlayerDraftHistory.season == season)
        if team_code:
            stmt = stmt.where(PlayerDraftHistory.team_code == team_code)

        records = list(session.execute(stmt.order_by(PlayerDraftHistory.pick_seq.asc())).scalars().all())
        results = [
            {
                "id": r.id,
                "season": r.season,
                "draft_type": r.draft_type,
                "round_num": r.round_num,
                "pick_seq": r.pick_seq,
                "team_code": r.team_code,
                "player_name": r.player_name,
                "player_id": r.player_id,
                "position": r.position,
                "school": r.school,
                "sign_fee": r.sign_fee,
            }
            for r in records
        ]

        return {"season": season, "count": len(results), "drafts": results}


@router.get(
    "/api/v1/players/{player_id}/splits",
    dependencies=[Depends(get_api_key)],
    response_model=PlayerSplitsResponse,
    summary="선수 상황별/스플릿 세부 통계 조회",
)
def get_player_splits(
    player_id: str,
    season: Annotated[int, Query(description="시즌 연도")] = 2026,
) -> dict[str, Any]:
    """Query player situational split statistics (득점권시, 좌우투수 상대 등)."""
    with get_db_session() as session:
        stmt = (
            select(PlayerSplitsStat)
            .where(PlayerSplitsStat.player_id == player_id, PlayerSplitsStat.season == season)
            .order_by(PlayerSplitsStat.split_type.asc())
        )
        records = list(session.execute(stmt).scalars().all())
        results = [
            {
                "season": r.season,
                "player_id": r.player_id,
                "player_name": r.player_name,
                "team_code": r.team_code,
                "split_type": r.split_type,
                "split_key": r.split_key,
                "ab": r.ab,
                "hits": r.hits,
                "hr": r.hr,
                "rbi": r.rbi,
                "bb": r.bb,
                "so": r.so,
                "avg": r.avg,
                "obp": r.obp,
                "slg": r.slg,
                "ops": r.ops,
            }
            for r in records
        ]

        return {"player_id": player_id, "season": season, "count": len(results), "splits": results}


@router.get(
    "/api/teams",
    dependencies=[Depends(get_api_key)],
    summary="KBO 구단 목록 조회",
)
@router.get(
    "/api/v1/teams",
    dependencies=[Depends(get_api_key)],
    summary="KBO 구단 목록 조회 (v1)",
)
def get_teams() -> dict[str, Any]:
    """Query list of KBO teams."""
    if _teams_state["data"] is not None and (time.monotonic() - _teams_state["ts"]) < _TEAMS_CACHE_TTL_SECONDS:
        return _teams_state["data"]

    return _build_teams_payload()
