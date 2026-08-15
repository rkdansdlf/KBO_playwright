"""Players and Teams API Router."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from src.aggregators.sabermetrics_calculator import SabermetricsCalculator
from src.api.auth import get_api_key
from src.api.cache import cached_api
from src.api.schemas import (
    PlayerBattingSeasonSchema,
    PlayerPitchingSeasonSchema,
    PlayerSabermetricsResponse,
    PlayerSeasonStatResponse,
    PlayerSplitsResponse,
)
from src.db.engine import get_db_session
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.player_draft import PlayerDraftHistory
from src.models.player_splits_stat import PlayerSplitsStat
from src.models.team import Team

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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
    "/api/v1/players/{player_id}/stats",
    dependencies=[Depends(get_api_key)],
    response_model=PlayerSeasonStatResponse,
    summary="선수 연도별/통산 시즌 성적 조회 (타격 및 투구)",
)
@cached_api(ttl_seconds=300.0, key_prefix="player_season_stats")
def get_player_stats(player_id: int) -> dict[str, Any]:
    """Query career season-by-season batting and pitching totals for a player."""
    with get_db_session() as session:
        player = session.get(PlayerBasic, player_id)
        if not player:
            raise HTTPException(status_code=404, detail=f"Player ID '{player_id}' not found")

        # Batting seasons
        bat_stmt = (
            select(PlayerSeasonBatting)
            .where(PlayerSeasonBatting.player_id == player_id)
            .order_by(PlayerSeasonBatting.season.desc())
        )
        batting_rows = list(session.execute(bat_stmt).scalars().all())
        bat_items = [
            PlayerBattingSeasonSchema(
                season=b.season,
                team_code=b.canonical_team_code or b.team_code,
                g=b.games or 0,
                pa=b.plate_appearances or 0,
                ab=b.at_bats or 0,
                r=b.runs or 0,
                h=b.hits or 0,
                two_b=b.doubles or 0,
                three_b=b.triples or 0,
                hr=b.home_runs or 0,
                rbi=b.rbi or 0,
                sb=b.stolen_bases or 0,
                cs=b.caught_stealing or 0,
                bb=b.walks or 0,
                so=b.strikeouts or 0,
                avg=b.avg,
                obp=b.obp,
                slg=b.slg,
                ops=b.ops,
            ).model_dump()
            for b in batting_rows
        ]

        # Pitching seasons
        pitch_stmt = (
            select(PlayerSeasonPitching)
            .where(PlayerSeasonPitching.player_id == player_id)
            .order_by(PlayerSeasonPitching.season.desc())
        )
        pitching_rows = list(session.execute(pitch_stmt).scalars().all())
        pitch_items = [
            PlayerPitchingSeasonSchema(
                season=p.season,
                team_code=p.canonical_team_code or p.team_code,
                g=p.games or 0,
                w=p.wins or 0,
                losses=p.losses or 0,
                sv=p.saves or 0,
                hld=p.holds or 0,
                ip=float(p.innings_pitched) if p.innings_pitched is not None else None,
                h=p.hits_allowed or 0,
                r=p.runs_allowed or 0,
                er=p.earned_runs or 0,
                bb=p.walks_allowed or 0,
                so=p.strikeouts or 0,
                hr=p.home_runs_allowed or 0,
                era=p.era,
                whip=p.whip,
            ).model_dump()
            for p in pitching_rows
        ]

        return {
            "player_id": str(player.player_id),
            "player_name": player.name,
            "position": player.position,
            "team": player.team,
            "batting_seasons": bat_items,
            "pitching_seasons": pitch_items,
        }


def _calc_player_sabermetrics(
    session: Session,
    season: int,
    bat_stat: PlayerSeasonBatting | None,
    pitch_stat: PlayerSeasonPitching | None,
) -> dict[str, float | int | None]:
    metrics: dict[str, float | int | None] = {
        "woba": None,
        "wraa": None,
        "wrc_plus": None,
        "ops_plus": None,
        "fip": None,
        "lob_pct": None,
        "batting_war": None,
        "pitching_war": None,
        "war": None,
        "babip": None,
        "isop": None,
    }
    league_constants: dict[str, Any] | None = None

    def _get_league_constants() -> dict[str, Any]:
        """Load league constants once when a metric calculation needs them."""
        nonlocal league_constants
        if league_constants is None:
            league_constants = SabermetricsCalculator.get_league_constants(session, season)
        return league_constants

    if bat_stat and (bat_stat.plate_appearances or 0) > 0:
        try:
            bat_metrics = SabermetricsCalculator.calculate_batting_metrics(bat_stat, _get_league_constants())
            metrics["woba"] = bat_metrics.get("woba")
            metrics["wraa"] = bat_metrics.get("wraa")
            metrics["wrc_plus"] = bat_metrics.get("wrc_plus")
            metrics["ops_plus"] = bat_metrics.get("ops_plus")
            metrics["batting_war"] = bat_metrics.get("war")
        except (ValueError, ZeroDivisionError, TypeError):
            logger.warning("Could not calculate full league sabermetrics; using row defaults", exc_info=True)

        metrics["babip"] = bat_stat.babip
        metrics["isop"] = bat_stat.iso

    if pitch_stat and (pitch_stat.innings_outs or 0) > 0:
        try:
            pitch_metrics = SabermetricsCalculator.calculate_pitching_metrics(pitch_stat, _get_league_constants())
            metrics["fip"] = pitch_metrics.get("fip_adj")
            metrics["lob_pct"] = pitch_metrics.get("lob_pct")
            metrics["pitching_war"] = pitch_metrics.get("war")
        except (ValueError, ZeroDivisionError, TypeError):
            logger.warning("Could not calculate pitching sabermetrics", exc_info=True)

    component_wars = [metrics["batting_war"], metrics["pitching_war"]]
    numeric_wars = [war for war in component_wars if isinstance(war, (int, float))]
    metrics["war"] = round(sum(numeric_wars), 2) if numeric_wars else None
    return metrics


@router.get(
    "/api/v1/players/{player_id}/sabermetrics",
    dependencies=[Depends(get_api_key)],
    response_model=PlayerSabermetricsResponse,
    summary="선수 시즌별 세이버메트릭스 지표 (wOBA, wRC+, WAR, FIP 등) 조회",
)
@cached_api(ttl_seconds=300.0, key_prefix="player_sabermetrics")
def get_player_sabermetrics(
    player_id: int,
    season: Annotated[int, Query(description="시즌 연도")] = 2026,
) -> dict[str, Any]:
    """Calculate and retrieve advanced sabermetric metrics for a player in a specific season."""
    with get_db_session() as session:
        player = session.get(PlayerBasic, player_id)
        if not player:
            raise HTTPException(status_code=404, detail=f"Player ID '{player_id}' not found")

        bat_stat = (
            session.query(PlayerSeasonBatting)
            .filter(PlayerSeasonBatting.player_id == player_id, PlayerSeasonBatting.season == season)
            .first()
        )
        pitch_stat = (
            session.query(PlayerSeasonPitching)
            .filter(PlayerSeasonPitching.player_id == player_id, PlayerSeasonPitching.season == season)
            .first()
        )

        if not bat_stat and not pitch_stat:
            raise HTTPException(
                status_code=404,
                detail=f"No stats found for player '{player_id}' in season {season}",
            )

        metrics = _calc_player_sabermetrics(session, season, bat_stat, pitch_stat)

        return {
            "player_id": str(player.player_id),
            "player_name": player.name,
            "season": season,
            **metrics,
        }


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
