"""FastAPI application for KBO Playwright REST API server."""

from __future__ import annotations

import csv
import io
import logging
from typing import Annotated, Any

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, UploadFile

from src.db.engine import get_db_session
from src.models.game import Game, GamePlayByPlay
from src.models.player import PlayerBasic
from src.utils.lock import ProcessLock

logger = logging.getLogger(__name__)

app = FastAPI(
    title="KBO Playwright Crawler API",
    description="REST API to monitor and control KBO data crawlers.",
    version="1.0.0",
)


def _check_lock_status(lock_name: str) -> bool:
    """Check if a ProcessLock is currently held by attempting a non-blocking acquire."""
    lock = ProcessLock(lock_name, blocking=False)
    if lock.acquire():
        lock.release()
        return False
    return True


@app.get("/health")
def health_check() -> dict[str, str]:
    """Provide a simple health check endpoint."""
    return {"status": "ok"}


@app.get("/status")
def get_system_status() -> dict[str, Any]:
    """Query database statistics and system lock statuses."""
    try:
        with get_db_session() as session:
            game_count = session.query(Game).count()
            player_count = session.query(PlayerBasic).count()
            latest_game = session.query(Game).order_by(Game.game_date.desc()).first()
            latest_game_date = str(latest_game.game_date) if latest_game else None

            # Get latest roster transaction
            from src.models.player import PlayerMovement

            latest_movement = session.query(PlayerMovement).order_by(PlayerMovement.created_at.desc()).first()
            latest_movement_at = latest_movement.created_at.isoformat() if latest_movement else None
    except Exception as e:
        logger.exception("Failed to query system status")
        raise HTTPException(status_code=500, detail=f"Database query failure: {e}") from e
    else:
        return {
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
                "realtime_oci_sync": _check_lock_status("realtime_oci_sync"),
            },
        }


def _async_run_daily_update() -> None:
    """Asynchronous background worker to execute daily update CLI."""
    from src.cli.run_daily_update import main as run_daily_update_main

    logger.info("[API] Starting background daily update crawl...")
    try:
        run_daily_update_main([])
        logger.info("[API] Background daily update crawl completed.")
    except Exception:
        logger.exception("[API] Background daily update crawl failed")


@app.post("/crawl/daily-update")
def trigger_daily_update(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Asynchronously triggers the daily update crawler pipeline."""
    if _check_lock_status("daily_update"):
        raise HTTPException(status_code=409, detail="Crawl already in progress (daily_update lock is held)")

    background_tasks.add_task(_async_run_daily_update)
    return {"status": "Daily update pipeline triggered in background"}


@app.post("/upload/text-relay")
async def upload_text_relay(file: Annotated[UploadFile, File()]) -> dict[str, Any]:
    """Upload and ingest a Naver Sports text-relay CSV file into the database."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    # Read uploaded file content
    contents = await file.read()
    try:
        decoded_content = contents.decode("utf-8")
    except UnicodeDecodeError:
        try:
            decoded_content = contents.decode("cp949")
        except UnicodeDecodeError as e:
            raise HTTPException(status_code=400, detail="Failed to decode CSV content (UTF-8 or CP949 required)") from e

    # Extract game ID from filename (e.g. "20260412SKLG0_text_relay.csv" or "20260412SKLG0.csv")
    stem = file.filename.rsplit(".", 1)[0]
    game_id = stem.replace("_text_relay", "").strip()
    if not game_id:
        raise HTTPException(status_code=400, detail="Could not determine game ID from filename")

    rows_inserted = 0
    try:
        with get_db_session() as session:
            # Delete existing play-by-plays for this game to prevent duplicates
            session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == game_id).delete()

            f = io.StringIO(decoded_content)
            reader = csv.DictReader(f)
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
                session.add(play)
                rows_inserted += 1
    except Exception as e:
        logger.exception("Failed to load text relay CSV upload")
        raise HTTPException(status_code=500, detail=f"CSV Ingestion failure: {e}") from e
    else:
        return {
            "status": "success",
            "game_id": game_id,
            "rows_inserted": rows_inserted,
        }
