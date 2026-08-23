"""FastAPI Router for KBO Sabermetrics, Player Advanced Stats, and BvP Matchups."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from src.api.auth import get_api_key
from src.api.dependencies import get_matchup_engine, get_sabermetrics_engine
from src.api.schemas import (
    BattingSabermetricsResponse,
    LeagueConstantsResponse,
    MatchupBvpResponse,
    PitchingSabermetricsResponse,
    SplitMetricsResponse,
)
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

if TYPE_CHECKING:
    from src.analytics.matchup import MatchupAnalyticsEngine
    from src.analytics.sabermetrics import SabermetricsEngine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analytics", tags=["KBO Sabermetrics & Matchup Analytics"])

SabermetricsEngineDep = Annotated["SabermetricsEngine", Depends(get_sabermetrics_engine)]
MatchupEngineDep = Annotated["MatchupAnalyticsEngine", Depends(get_matchup_engine)]


@router.get(
    "/constants",
    response_model=LeagueConstantsResponse,
    dependencies=[Depends(get_api_key)],
    summary="KBO 리그 연도별 세이버메트릭스 가중치 및 상수 조회",
)
def get_league_constants(
    engine: SabermetricsEngineDep,
    year: Annotated[int, Query(ge=1982, le=2100, description="시즌 연도")],
    level: Annotated[str, Query(description="리그 레벨 (KBO1, KBO2)")] = "KBO1",
) -> LeagueConstantsResponse:
    """Retrieve league-wide sabermetric weights, league wOBA, and FIP constant."""
    try:
        consts = engine.get_league_constants(year, level)
        return LeagueConstantsResponse(**consts.to_dict())
    except Exception as exc:
        logger.exception("Failed to compute league constants for %s %s", year, level)
        raise HTTPException(status_code=500, detail=f"Failed to calculate league constants: {exc}") from exc


@router.get(
    "/batting",
    response_model=list[BattingSabermetricsResponse],
    dependencies=[Depends(get_api_key)],
    summary="타자 세이버메트릭스 고급 지표 조회 (wOBA, wRC+, WAR, ISO, BABIP)",
)
def get_batting_sabermetrics(
    engine: SabermetricsEngineDep,
    year: Annotated[int, Query(ge=1982, le=2100, description="시즌 연도")],
    player_id: Annotated[int | None, Query(description="선수 ID 필터")] = None,
    level: Annotated[str, Query(description="리그 레벨")] = "KBO1",
) -> list[BattingSabermetricsResponse]:
    """Calculate and return advanced batting sabermetrics for a season or player."""
    try:
        consts = engine.get_league_constants(year, level)
        query = engine.session.query(PlayerSeasonBatting).filter(
            PlayerSeasonBatting.season == year,
            PlayerSeasonBatting.level == level,
        )
        if player_id:
            query = query.filter(PlayerSeasonBatting.player_id == player_id)

        rows = query.all()
        results: list[BattingSabermetricsResponse] = []
        for r in rows:
            m = engine.calculate_batting_metrics(r, consts)
            results.append(BattingSabermetricsResponse(**m.to_dict()))
    except Exception as exc:
        logger.exception("Failed to retrieve batting sabermetrics")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    else:
        return results


@router.get(
    "/pitching",
    response_model=list[PitchingSabermetricsResponse],
    dependencies=[Depends(get_api_key)],
    summary="투수 세이버메트릭스 고급 지표 조회 (FIP, kFIP, ERA+, WHIP, WAR)",
)
def get_pitching_sabermetrics(
    engine: SabermetricsEngineDep,
    year: Annotated[int, Query(ge=1982, le=2100, description="시즌 연도")],
    player_id: Annotated[int | None, Query(description="선수 ID 필터")] = None,
    level: Annotated[str, Query(description="리그 레벨")] = "KBO1",
) -> list[PitchingSabermetricsResponse]:
    """Calculate and return advanced pitching sabermetrics for a season or player."""
    try:
        consts = engine.get_league_constants(year, level)
        query = engine.session.query(PlayerSeasonPitching).filter(
            PlayerSeasonPitching.season == year,
            PlayerSeasonPitching.level == level,
        )
        if player_id:
            query = query.filter(PlayerSeasonPitching.player_id == player_id)

        rows = query.all()
        results: list[PitchingSabermetricsResponse] = []
        for r in rows:
            m = engine.calculate_pitching_metrics(r, consts)
            results.append(PitchingSabermetricsResponse(**m.to_dict()))
    except Exception as exc:
        logger.exception("Failed to retrieve pitching sabermetrics")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    else:
        return results


@router.get(
    "/matchup/bvp",
    response_model=list[MatchupBvpResponse],
    dependencies=[Depends(get_api_key)],
    summary="타자 vs 투수 (BvP) 상대 전적 매트릭스 조회",
)
def get_bvp_matchups(
    engine: MatchupEngineDep,
    year: Annotated[int, Query(ge=1982, le=2100, description="시즌 연도")],
    batter_id: Annotated[int | None, Query(description="타자 ID")] = None,
    pitcher_id: Annotated[int | None, Query(description="투수 ID")] = None,
) -> list[MatchupBvpResponse]:
    """Retrieve Batter vs. Pitcher Head-to-Head matchup stats from play-by-play."""
    try:
        matrix = engine.calculate_bvp_matchups(year)
        if batter_id:
            matrix = [m for m in matrix if m.batter_id == batter_id]
        if pitcher_id:
            matrix = [m for m in matrix if m.pitcher_id == pitcher_id]
        return [MatchupBvpResponse(**m.to_dict()) for m in matrix]
    except Exception as exc:
        logger.exception("Failed to calculate BvP matchups")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/splits/risp",
    response_model=list[SplitMetricsResponse],
    dependencies=[Depends(get_api_key)],
    summary="타자 득점권 (RISP) 상황별 스플릿 조회",
)
def get_risp_splits(
    engine: MatchupEngineDep,
    year: Annotated[int, Query(ge=1982, le=2100, description="시즌 연도")],
    batter_id: Annotated[int | None, Query(description="타자 ID")] = None,
) -> list[SplitMetricsResponse]:
    """Retrieve RISP situational split statistics."""
    try:
        splits = engine.calculate_situational_splits(year)
        if batter_id:
            splits = [s for s in splits if s.entity_id == batter_id]
        return [SplitMetricsResponse(**s.to_dict()) for s in splits]
    except Exception as exc:
        logger.exception("Failed to calculate RISP splits")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
