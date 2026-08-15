"""Games, Standings, Text-Relay and Daily Update Router."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy import or_, select

from src.api.auth import get_api_key
from src.api.cache import api_cache, cached_api
from src.api.schemas import (
    GameBoxscoreResponse,
    GameHighlightItemSchema,
    GameHighlightsResponse,
    GameLineupPlayerSchema,
    HeadToHeadGameItemSchema,
    HeadToHeadResponse,
    HitterBoxscoreSchema,
    InningScoreSchema,
    PitcherBoxscoreSchema,
    WpaChartResponse,
)
from src.constants import DATE_STR_LEN, KST
from src.db.engine import get_db_session
from src.models.game import (
    Game,
    GameBattingStat,
    GameHighlight,
    GameInningScore,
    GameLineup,
    GamePitchingStat,
    GamePlayByPlay,
)
from src.models.standings import TeamStandingsDaily
from src.utils.job_tracker import job_tracker
from src.utils.lock import ProcessLock

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Games & Standings"])


def _check_lock_status(lock_name: str) -> bool:
    lock = ProcessLock(lock_name, blocking=False)
    if lock.acquire():
        lock.release()
        return False
    return True


def _async_run_daily_update(job_id: str) -> None:
    from src.cli.run_daily_update import main as run_daily_update_main

    logger.info("[API] Starting background daily update crawl for job %s...", job_id)
    try:
        run_daily_update_main([])
        api_cache.clear()
        job_tracker.complete_job(job_id, {"status": "success"})
        logger.info("[API] Background daily update crawl completed for job %s.", job_id)
    except Exception as e:
        job_tracker.fail_job(job_id, str(e))
        logger.exception("[API] Background daily update crawl failed for job %s", job_id)


@router.get("/api/games", dependencies=[Depends(get_api_key)])
@router.get("/api/v1/games", dependencies=[Depends(get_api_key)])
def get_games(
    season: Annotated[int | None, Query(description="시즌 연도 (예: 2025)")] = None,
    date: Annotated[str | None, Query(description="경기 날짜 (YYYY-MM-DD 또는 YYYYMMDD)")] = None,
    team: Annotated[str | None, Query(description="팀 코드 (예: KIA, LG, SSG)")] = None,
    limit: Annotated[int, Query(ge=1, le=500, description="조회 개수 제한")] = 50,
    offset: Annotated[int, Query(ge=0, description="오프셋")] = 0,
) -> dict[str, Any]:
    """Query KBO game list with optional filters."""
    try:
        with get_db_session() as session:
            query = session.query(Game)
            if season is not None:
                query = query.filter(Game.season_id == season)
            if date:
                clean_date = date.replace("-", "")
                if len(clean_date) == DATE_STR_LEN:
                    d_obj = datetime.strptime(clean_date, "%Y%m%d").replace(tzinfo=KST).date()
                    query = query.filter(Game.game_date == d_obj)
            if team:
                query = query.filter((Game.home_team == team) | (Game.away_team == team))

            total_count = query.count()
            games = query.order_by(Game.game_date.desc(), Game.game_id).offset(offset).limit(limit).all()

            results = [
                {
                    "game_id": g.game_id,
                    "game_date": str(g.game_date) if g.game_date else None,
                    "stadium": g.stadium,
                    "home_team": g.home_team,
                    "away_team": g.away_team,
                    "home_score": g.home_score,
                    "away_score": g.away_score,
                    "winning_team": g.winning_team,
                    "game_status": g.game_status,
                    "season_id": g.season_id,
                }
                for g in games
            ]
            return {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "games": results,
            }
    except Exception as e:
        logger.exception("Failed to query games")
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}") from e


@router.get(
    "/api/v1/games/{game_id}/boxscore",
    dependencies=[Depends(get_api_key)],
    response_model=GameBoxscoreResponse,
    summary="경기 상세 박스스코어 (이닝 스코어보드, 타자/투수 기록)",
)
def get_game_boxscore(game_id: str) -> dict[str, Any]:
    """Query comprehensive game boxscore including scoreboard, batting, and pitching lines."""
    cache_key = f"game_boxscore:{game_id}"
    cached_val = api_cache.get(cache_key)
    if cached_val is not None and isinstance(cached_val, dict):
        return cached_val

    with get_db_session() as session:
        stmt = select(Game).where(Game.game_id == game_id)
        game = session.execute(stmt).scalar_one_or_none()
        if not game:
            raise HTTPException(status_code=404, detail=f"Game '{game_id}' not found")

        # 1. Scoreboard (Innings)
        innings_stmt = (
            select(GameInningScore)
            .where(GameInningScore.game_id == game_id)
            .order_by(GameInningScore.team_side.asc(), GameInningScore.inning.asc())
        )
        innings = list(session.execute(innings_stmt).scalars().all())

        scoreboard_map: dict[str, list[int]] = {"away": [], "home": []}
        for inn in innings:
            side = inn.team_side or "away"
            if side in scoreboard_map:
                scoreboard_map[side].append(inn.runs or 0)

        scoreboard = [
            InningScoreSchema(
                team=game.away_team or "AWAY",
                scores=scoreboard_map["away"] or [0],
                r=game.away_score or 0,
                h=sum(b.hits for b in game.batting_stats if b.team_side == "away") if game.batting_stats else 0,
                e=0,
                b=sum(b.walks for b in game.batting_stats if b.team_side == "away") if game.batting_stats else 0,
            ).model_dump(),
            InningScoreSchema(
                team=game.home_team or "HOME",
                scores=scoreboard_map["home"] or [0],
                r=game.home_score or 0,
                h=sum(b.hits for b in game.batting_stats if b.team_side == "home") if game.batting_stats else 0,
                e=0,
                b=sum(b.walks for b in game.batting_stats if b.team_side == "home") if game.batting_stats else 0,
            ).model_dump(),
        ]

        lineup_stmt = (
            select(GameLineup)
            .where(GameLineup.game_id == game_id)
            .order_by(GameLineup.team_side.asc(), GameLineup.batting_order.asc(), GameLineup.appearance_seq.asc())
        )
        lineup_rows = list(session.execute(lineup_stmt).scalars().all())
        away_lineup = [
            GameLineupPlayerSchema(
                order=row.batting_order,
                player_id=str(row.player_id) if row.player_id else None,
                player_name=row.player_name,
                position=row.position or row.standard_position,
                is_starter=bool(row.is_starter),
            ).model_dump()
            for row in lineup_rows
            if row.team_side == "away"
        ]
        home_lineup = [
            GameLineupPlayerSchema(
                order=row.batting_order,
                player_id=str(row.player_id) if row.player_id else None,
                player_name=row.player_name,
                position=row.position or row.standard_position,
                is_starter=bool(row.is_starter),
            ).model_dump()
            for row in lineup_rows
            if row.team_side == "home"
        ]

        # 2. Batting Stats
        bat_stmt = (
            select(GameBattingStat)
            .where(GameBattingStat.game_id == game_id)
            .order_by(GameBattingStat.appearance_seq.asc())
        )
        batting_rows = list(session.execute(bat_stmt).scalars().all())

        away_batters = [
            HitterBoxscoreSchema(
                order=b.batting_order,
                player_id=str(b.player_id) if b.player_id else None,
                player_name=b.player_name,
                position=b.position or b.standard_position,
                ab=b.at_bats or 0,
                r=b.runs or 0,
                h=b.hits or 0,
                rbi=b.rbi or 0,
                bb=b.walks or 0,
                so=b.strikeouts or 0,
                avg=b.avg,
            ).model_dump()
            for b in batting_rows
            if b.team_side == "away"
        ]

        home_batters = [
            HitterBoxscoreSchema(
                order=b.batting_order,
                player_id=str(b.player_id) if b.player_id else None,
                player_name=b.player_name,
                position=b.position or b.standard_position,
                ab=b.at_bats or 0,
                r=b.runs or 0,
                h=b.hits or 0,
                rbi=b.rbi or 0,
                bb=b.walks or 0,
                so=b.strikeouts or 0,
                avg=b.avg,
            ).model_dump()
            for b in batting_rows
            if b.team_side == "home"
        ]

        # 3. Pitching Stats
        pitch_stmt = (
            select(GamePitchingStat)
            .where(GamePitchingStat.game_id == game_id)
            .order_by(GamePitchingStat.appearance_seq.asc())
        )
        pitching_rows = list(session.execute(pitch_stmt).scalars().all())

        away_pitchers = [
            PitcherBoxscoreSchema(
                order=p.appearance_seq,
                player_id=str(p.player_id) if p.player_id else None,
                player_name=p.player_name,
                decision=p.decision,
                innings=str(p.innings_pitched) if p.innings_pitched is not None else None,
                h=p.hits_allowed or 0,
                r=p.runs_allowed or 0,
                er=p.earned_runs or 0,
                bb=p.walks_allowed or 0,
                so=p.strikeouts or 0,
                hr=p.home_runs_allowed or 0,
                era=p.era,
            ).model_dump()
            for p in pitching_rows
            if p.team_side == "away"
        ]

        home_pitchers = [
            PitcherBoxscoreSchema(
                order=p.appearance_seq,
                player_id=str(p.player_id) if p.player_id else None,
                player_name=p.player_name,
                decision=p.decision,
                innings=str(p.innings_pitched) if p.innings_pitched is not None else None,
                h=p.hits_allowed or 0,
                r=p.runs_allowed or 0,
                er=p.earned_runs or 0,
                bb=p.walks_allowed or 0,
                so=p.strikeouts or 0,
                hr=p.home_runs_allowed or 0,
                era=p.era,
            ).model_dump()
            for p in pitching_rows
            if p.team_side == "home"
        ]

        highlights_stmt = (
            select(GameHighlight)
            .where(GameHighlight.game_id == game_id)
            .order_by(GameHighlight.importance_score.desc(), GameHighlight.event_seq.asc())
        )
        highlight_rows = list(session.execute(highlights_stmt).scalars().all())
        highlights = [
            GameHighlightItemSchema(
                id=row.id,
                game_id=row.game_id,
                event_seq=row.event_seq,
                inning=row.inning,
                inning_half=row.inning_half,
                highlight_type=row.highlight_type,
                description=row.description,
                wpa=row.wpa,
                importance_score=row.importance_score,
                tags=row.tags or [],
            ).model_dump()
            for row in highlight_rows
        ]

        status_upper = (game.game_status or "").upper()
        is_finished = status_upper in {"FINAL", "COMPLETED", "CANCELLED", "TERMINAL", "종료"}
        cache_ttl = 600.0 if is_finished else 5.0

        result = {
            "game_id": game.game_id,
            "game_date": str(game.game_date),
            "stadium": game.stadium or "",
            "home_team": game.home_team or "",
            "away_team": game.away_team or "",
            "home_score": game.home_score or 0,
            "away_score": game.away_score or 0,
            "game_status": game.game_status or "COMPLETED",
            "scoreboard": scoreboard,
            "away_lineup": away_lineup,
            "home_lineup": home_lineup,
            "away_batters": away_batters,
            "home_batters": home_batters,
            "away_pitchers": away_pitchers,
            "home_pitchers": home_pitchers,
            "highlights": highlights,
        }
        api_cache.set(cache_key, result, ttl_seconds=cache_ttl)
        return result


def _summarize_h2h_game(
    g: Game,
    team1: str,
    limit: int,
    recent_items: list[dict[str, Any]],
) -> tuple[int, int, int, int, int, int]:
    """Summarize a single game record for head-to-head aggregation."""
    h_score = g.home_score if g.home_score is not None else 0
    a_score = g.away_score if g.away_score is not None else 0
    t1_score, t2_score = (h_score, a_score) if g.home_team == team1 else (a_score, h_score)

    t1_w, t2_w, draw, valid_cnt = 0, 0, 0, 0
    if g.home_score is not None and g.away_score is not None:
        valid_cnt = 1
        if t1_score > t2_score:
            t1_w = 1
        elif t2_score > t1_score:
            t2_w = 1
        else:
            draw = 1

    if g.home_score is not None and g.away_score is not None and len(recent_items) < limit:
        winner = g.winning_team or (g.home_team if h_score > a_score else g.away_team if a_score > h_score else None)
        recent_items.append(
            HeadToHeadGameItemSchema(
                game_id=g.game_id,
                game_date=str(g.game_date),
                home_team=g.home_team or "",
                away_team=g.away_team or "",
                home_score=h_score,
                away_score=a_score,
                winner=winner,
            ).model_dump()
        )

    return t1_w, t2_w, draw, t1_score, t2_score, valid_cnt


@router.get(
    "/api/v1/games/head-to-head",
    dependencies=[Depends(get_api_key)],
    response_model=HeadToHeadResponse,
    summary="두 구단 간 상대 전적 (Head-to-Head) 조회",
)
@cached_api(ttl_seconds=300.0, key_prefix="head_to_head")
def get_head_to_head(
    team1: Annotated[str, Query(description="첫 번째 팀 코드 (예: KIA)")],
    team2: Annotated[str, Query(description="두 번째 팀 코드 (예: LG)")],
    season: Annotated[int | None, Query(description="시즌 연도 (기본: 전체 시즌)")] = None,
    limit: Annotated[int, Query(ge=1, le=50, description="최근 경기 개수 제한")] = 10,
) -> dict[str, Any]:
    """Query head-to-head matchups, win-loss records, and recent games between two teams."""
    try:
        with get_db_session() as session:
            stmt = select(Game).where(
                or_(
                    (Game.home_team == team1) & (Game.away_team == team2),
                    (Game.home_team == team2) & (Game.away_team == team1),
                )
            )
            if season is not None:
                stmt = stmt.where(Game.season_id == season)

            games = list(session.execute(stmt.order_by(Game.game_date.desc())).scalars().all())

            t1_wins, t2_wins, draws = 0, 0, 0
            t1_runs, t2_runs, valid_count = 0, 0, 0
            recent_items: list[dict[str, Any]] = []

            for g in games:
                w1, w2, d, r1, r2, v = _summarize_h2h_game(g, team1, limit, recent_items)
                t1_wins += w1
                t2_wins += w2
                draws += d
                t1_runs += r1
                t2_runs += r2
                valid_count += v

            t1_avg = round(t1_runs / valid_count, 2) if valid_count > 0 else 0.0
            t2_avg = round(t2_runs / valid_count, 2) if valid_count > 0 else 0.0

            return {
                "team1": team1,
                "team2": team2,
                "season": season,
                "team1_wins": t1_wins,
                "team2_wins": t2_wins,
                "draws": draws,
                "total_games": valid_count,
                "team1_avg_runs": t1_avg,
                "team2_avg_runs": t2_avg,
                "recent_games": recent_items,
            }
    except Exception as e:
        logger.exception("Failed to query head-to-head for %s vs %s", team1, team2)
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}") from e


@router.get("/api/standings", dependencies=[Depends(get_api_key)])
@router.get("/api/v1/standings", dependencies=[Depends(get_api_key)])
@cached_api(ttl_seconds=300.0, key_prefix="standings")
def get_team_standings(
    season: Annotated[int | None, Query(description="시즌 연도")] = None,
    date: Annotated[str | None, Query(description="조회 기준일 (YYYY-MM-DD 또는 YYYYMMDD)")] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
) -> dict[str, Any]:
    """Query daily team standings data."""
    try:
        with get_db_session() as session:
            query = session.query(TeamStandingsDaily)
            if date:
                clean_date = date.replace("-", "")
                if len(clean_date) == DATE_STR_LEN:
                    d_obj = datetime.strptime(clean_date, "%Y%m%d").replace(tzinfo=KST).date()
                    query = query.filter(TeamStandingsDaily.standings_date == d_obj)
            elif season:
                from datetime import date as dt_date

                query = query.filter(TeamStandingsDaily.standings_date >= dt_date(season, 1, 1)).filter(
                    TeamStandingsDaily.standings_date <= dt_date(season, 12, 31)
                )

            latest_item = query.order_by(TeamStandingsDaily.standings_date.desc()).first()
            if not latest_item:
                return {"date": None, "standings": []}

            target_date = latest_item.standings_date
            standings = (
                session.query(TeamStandingsDaily)
                .filter(TeamStandingsDaily.standings_date == target_date)
                .order_by(TeamStandingsDaily.rank.asc())
                .limit(limit)
                .all()
            )

            results = [
                {
                    "rank": s.rank,
                    "team_code": s.team_code,
                    "games_played": s.games_played,
                    "wins": s.wins,
                    "losses": s.losses,
                    "draws": s.draws,
                    "win_pct": s.win_pct,
                    "games_behind": s.games_behind,
                    "current_streak": s.current_streak,
                    "runs_scored": s.runs_scored,
                    "runs_allowed": s.runs_allowed,
                    "run_differential": s.run_differential,
                }
                for s in standings
            ]
            return {
                "date": str(target_date),
                "standings": results,
            }
    except Exception as e:
        logger.exception("Failed to query team standings")
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}") from e


@router.post("/crawl/daily-update", status_code=202, dependencies=[Depends(get_api_key)])
def trigger_daily_update(background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Asynchronously triggers the daily update crawler pipeline (HTTP 202 Accepted)."""
    if _check_lock_status("daily_update"):
        raise HTTPException(status_code=409, detail="Crawl already in progress (daily_update lock is held)")

    job_id = job_tracker.create_job("daily_update")
    background_tasks.add_task(_async_run_daily_update, job_id)
    return {
        "status": "accepted",
        "job_id": job_id,
        "message": "Daily update pipeline triggered in background",
    }


@router.get("/api/v1/jobs/status/{job_id}", dependencies=[Depends(get_api_key)])
def get_job_status(job_id: str) -> dict[str, Any]:
    """Query background job progress and completion status."""
    job = job_tracker.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@router.post("/upload/text-relay", dependencies=[Depends(get_api_key)])
async def upload_text_relay(file: Annotated[UploadFile, File()]) -> dict[str, Any]:
    """Upload and ingest a Naver Sports text-relay CSV file into the database."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    contents = await file.read()
    try:
        decoded_content = contents.decode("utf-8")
    except UnicodeDecodeError:
        try:
            decoded_content = contents.decode("cp949")
        except UnicodeDecodeError as e:
            raise HTTPException(status_code=400, detail="Failed to decode CSV content (UTF-8 or CP949 required)") from e

    stem = file.filename.rsplit(".", 1)[0]
    game_id = stem.replace("_text_relay", "").strip()
    if not game_id:
        raise HTTPException(status_code=400, detail="Could not determine game ID from filename")

    rows_inserted = 0
    try:
        with get_db_session() as session:
            session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == game_id).delete()

            f = io.StringIO(decoded_content)
            reader = csv.DictReader(f)
            plays = []
            for idx, row in enumerate(reader):
                play = GamePlayByPlay(
                    game_id=game_id,
                    inning=row.get("inning"),
                    inning_half=row.get("inning_half"),
                    pitcher_name=row.get("pitcher_name"),
                    batter_name=row.get("batter_name"),
                    play_description=row.get("play_description", ""),
                    event_type=row.get("event_type"),
                    result=row.get("result"),
                    source_name="text_relay_upload_api",
                    source_row_index=idx,
                )
                plays.append(play)
            session.add_all(plays)
            rows_inserted = len(plays)
    except Exception as e:
        logger.exception("Failed to load text relay CSV upload")
        raise HTTPException(status_code=500, detail=f"CSV Ingestion failure: {e}") from e
    else:
        api_cache.clear()
        return {
            "status": "success",
            "game_id": game_id,
            "rows_inserted": rows_inserted,
        }


@router.get(
    "/api/v1/games/{game_id}/wpa",
    dependencies=[Depends(get_api_key)],
    response_model=WpaChartResponse,
    summary="경기 실시간 승리 확률(WPA) 시계열 및 주요 모멘텀 승부처 조회",
)
def get_game_wpa(
    game_id: str,
    top_n: Annotated[int, Query(ge=1, le=20, description="추출할 주요 승부처(터닝포인트) 개수")] = 5,
) -> dict[str, Any]:
    """Query Win Expectancy timeline and top momentum turning points for a game."""
    cache_key = f"game_wpa:{game_id}:{top_n}"
    cached_val = api_cache.get(cache_key)
    if cached_val is not None and isinstance(cached_val, dict):
        return cached_val

    with get_db_session() as session:
        from src.services.wpa_chart_service import WpaChartService

        service = WpaChartService(session)
        chart_data = service.get_game_wpa_chart(game_id, top_turning_points=top_n)
        if not chart_data:
            raise HTTPException(status_code=404, detail=f"Game '{game_id}' not found")

        status_upper = (chart_data.get("game_status") or "").upper()
        is_finished = status_upper in {"FINAL", "COMPLETED", "CANCELLED", "TERMINAL", "종료"}
        cache_ttl = 600.0 if is_finished else 5.0

        api_cache.set(cache_key, chart_data, ttl_seconds=cache_ttl)
        return chart_data


@router.get(
    "/api/v1/games/{game_id}/highlights",
    dependencies=[Depends(get_api_key)],
    response_model=GameHighlightsResponse,
    summary="경기 주요 하이라이트 및 결정적 플레이 목록 조회",
)
@cached_api(ttl_seconds=300.0, key_prefix="game_highlights")
def get_game_highlights(game_id: str) -> dict[str, Any]:
    """Query game highlights and key moments."""
    with get_db_session() as session:
        from src.services.wpa_chart_service import WpaChartService

        service = WpaChartService(session)
        highlights = service.get_game_highlights(game_id)
        return {
            "game_id": game_id,
            "count": len(highlights),
            "highlights": highlights,
        }
