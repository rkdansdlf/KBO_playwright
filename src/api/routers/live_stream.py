"""FastAPI router for real-time WebSocket live game streaming and simulation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, BackgroundTasks, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from src.api.websocket_manager import ws_manager
from src.simulation.live_stream_processor import LiveStreamProcessor
from src.simulation.stream_generator import GameStreamGenerator

if TYPE_CHECKING:
    from src.simulation.dto import SimulationEvent

logger = logging.getLogger(__name__)

router = APIRouter(tags=["KBO Live Stream & WebSocket"])


class SimulateGameRequest(BaseModel):
    """Request payload for triggering an interactive live game simulation stream."""

    home_team: str = Field(default="KIA", description="Home team code")
    away_team: str = Field(default="LG", description="Away team code")
    innings: int = Field(default=9, ge=1, le=15, description="Max regulation/extra innings")
    speed: float = Field(default=10.0, ge=0.0, le=100.0, description="Simulation speed multiplier")
    seed: int | None = Field(default=None, description="Deterministic RNG seed")


@router.websocket("/ws/live/{game_id}")
async def websocket_live_game_endpoint(websocket: WebSocket, game_id: str) -> None:
    """WebSocket endpoint for subscribing to real-time play-by-play events and WPA updates."""
    await ws_manager.connect(websocket, game_id)
    try:
        # Send initial connection handshake payload
        await websocket.send_json(
            {
                "type": "CONNECTION_ESTABLISHED",
                "game_id": game_id,
                "active_viewers": ws_manager.get_active_count(game_id),
                "message": f"Subscribed to real-time stream for game {game_id}",
            }
        )

        while True:
            # Maintain connection and listen for client messages (e.g. ping/pong)
            data = await websocket.receive_text()
            if data.lower() == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, game_id)
        logger.info("Client disconnected from WebSocket stream: %s", game_id)
    except Exception as e:  # noqa: BLE001
        ws_manager.disconnect(websocket, game_id)
        logger.warning("WebSocket connection exception for %s: %s", game_id, e)


def _run_simulation_task(game_id: str, req: SimulateGameRequest) -> None:
    """Execute simulation worker and push events into active loop for WebSocket broadcast."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    generator = GameStreamGenerator(
        home_team=req.home_team,
        away_team=req.away_team,
        seed=req.seed,
    )
    events = generator.generate_game_stream(game_id=game_id, max_innings=req.innings)

    def _on_event(ev: SimulationEvent) -> None:
        payload = {
            "type": "PLAY_EVENT",
            "game_id": game_id,
            "event": ev.to_dict(),
        }
        loop.run_until_complete(ws_manager.broadcast_to_game(game_id, payload))

    processor = LiveStreamProcessor()
    summary = processor.process_stream(
        events=events,
        game_id=game_id,
        home_team=req.home_team,
        away_team=req.away_team,
        speed_multiplier=req.speed,
        notify_hot_moments=False,
        event_callback=_on_event,
    )

    final_payload = {
        "type": "GAME_FINISHED",
        "game_id": game_id,
        "summary": summary.to_dict(),
    }
    loop.run_until_complete(ws_manager.broadcast_to_game(game_id, final_payload))
    loop.close()


@router.post("/api/v1/live/{game_id}/simulate")
async def trigger_live_simulation(
    game_id: str,
    background_tasks: BackgroundTasks,
    req: SimulateGameRequest | None = None,
) -> dict[str, Any]:
    """Trigger an interactive game simulation that broadcasts events over WebSocket."""
    payload = req or SimulateGameRequest()

    background_tasks.add_task(
        _run_simulation_task,
        game_id=game_id,
        req=payload,
    )

    return {
        "status": "SIMULATION_TRIGGERED",
        "game_id": game_id,
        "home_team": payload.home_team,
        "away_team": payload.away_team,
        "innings": payload.innings,
        "speed": payload.speed,
        "websocket_url": f"/ws/live/{game_id}",
        "active_viewers": ws_manager.get_active_count(game_id),
    }


@router.get("/api/v1/live/stats")
async def get_live_stream_stats() -> dict[str, Any]:
    """Get active WebSocket connection counts and monitoring metrics."""
    return {
        "active_global_clients": ws_manager.get_active_count(),
        "channels": ws_manager.get_all_active_counts(),
    }


__all__ = ["router"]
