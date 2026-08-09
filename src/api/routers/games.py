"""Games, Standings, Text-Relay and Daily Update Router."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile

from src.api.auth import get_api_key
from src.constants import DATE_STR_LEN, KST
from src.db.engine import get_db_session
from src.models.game import Game, GamePlayByPlay
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
        job_tracker.complete_job(job_id, {"status": "success"})
        logger.info("[API] Background daily update crawl completed for job %s.", job_id)
    except Exception as e:
        job_tracker.fail_job(job_id, str(e))
        logger.exception("[API] Background daily update crawl failed for job %s", job_id)


@router.get("/api/games", dependencies=[Depends(get_api_key)])
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


@router.get("/api/standings", dependencies=[Depends(get_api_key)])
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
        return {
            "status": "success",
            "game_id": game_id,
            "rows_inserted": rows_inserted,
        }
