"""FastAPI router for real-time WebSocket live game streaming, multi-game channels, and circuit breaker."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from src.api.circuit_breaker import AsyncCircuitBreaker
from src.api.live_stream_dto import (
    CircuitActionResponse,
    CircuitState,
    CircuitStatusDTO,
    LiveChannelStatsDTO,
    LiveStreamStatsResponse,
    StreamEventType,
)
from src.api.websocket_manager import ws_manager
from src.simulation.stream_generator import GameStreamGenerator

logger = logging.getLogger(__name__)

router = APIRouter(tags=["KBO Live Stream & WebSocket"])


async def _on_circuit_state_change(name: str, old_state: CircuitState, new_state: CircuitState) -> None:
    """Broadcast circuit state transition events to all connected WebSocket clients."""
    payload = {
        "type": StreamEventType.CIRCUIT_STATE_CHANGED.value,
        "circuit_name": name,
        "previous_state": old_state.value,
        "new_state": new_state.value,
        "is_degraded": new_state != CircuitState.CLOSED,
        "message": f"Circuit '{name}' transitioned from {old_state.value} to {new_state.value}",
    }
    await ws_manager.broadcast_global(payload)


# Global Circuit Breaker instance for Live Relay & Stream Engine
live_relay_breaker = AsyncCircuitBreaker(
    name="kbo_live_relay_stream",
    failure_threshold=3,
    recovery_timeout_seconds=20.0,
    half_open_success_threshold=1,
    on_state_change=_on_circuit_state_change,
)


class SimulateGameRequest(BaseModel):
    """Request payload for triggering an interactive live game simulation stream."""

    home_team: str = Field(default="KIA", description="Home team code")
    away_team: str = Field(default="LG", description="Away team code")
    innings: int = Field(default=9, ge=1, le=15, description="Max regulation/extra innings")
    speed: float = Field(default=10.0, ge=0.0, le=100.0, description="Simulation speed multiplier")
    seed: int | None = Field(default=None, description="Deterministic RNG seed")


class ManualTripRequest(BaseModel):
    """Request payload for manually tripping a circuit breaker."""

    reason: str = Field(default="Manual maintenance trip", description="Reason for tripping the circuit")


async def _handle_multi_channel_message(websocket: WebSocket, data_text: str) -> None:
    """Parse and dispatch dynamic multi-channel subscription actions."""
    if data_text.lower() == "ping":
        await websocket.send_text("pong")
        return

    try:
        data_json = json.loads(data_text)
        action = str(data_json.get("action", "")).lower()
        games = data_json.get("games", [])
        if isinstance(games, str):
            games = [games]

        if action == "subscribe":
            for gid in games:
                ws_manager.subscribe(websocket, gid)
            await websocket.send_json(
                {
                    "type": StreamEventType.CHANNEL_SUBSCRIBED.value,
                    "subscribed_games": games,
                    "active_viewers": {gid: ws_manager.get_active_count(gid) for gid in games},
                }
            )
        elif action == "unsubscribe":
            for gid in games:
                ws_manager.unsubscribe(websocket, gid)
            await websocket.send_json(
                {
                    "type": StreamEventType.CHANNEL_UNSUBSCRIBED.value,
                    "unsubscribed_games": games,
                }
            )
        else:
            await websocket.send_json(
                {
                    "type": StreamEventType.ERROR.value,
                    "message": f"Unknown action: '{action}'. Expected 'subscribe' or 'unsubscribe'",
                }
            )
    except json.JSONDecodeError:
        await websocket.send_json(
            {
                "type": StreamEventType.ERROR.value,
                "message": "Invalid JSON message received",
            }
        )


@router.websocket("/ws/live/multi")
async def websocket_multi_game_endpoint(websocket: WebSocket) -> None:
    """WebSocket endpoint for subscribing to multiple games simultaneously with dynamic JSON actions."""
    await ws_manager.connect(websocket)
    try:
        await websocket.send_json(
            {
                "type": StreamEventType.CONNECTION_ESTABLISHED.value,
                "message": "Connected to KBO Multi-Game Live Stream Broker",
                "circuit_state": live_relay_breaker.state.value,
            }
        )

        while True:
            data_text = await websocket.receive_text()
            await _handle_multi_channel_message(websocket, data_text)
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
        logger.info("Multi-game WebSocket client disconnected")
    except Exception as e:  # noqa: BLE001
        ws_manager.disconnect(websocket)
        logger.warning("Multi-game WebSocket exception: %s", e)


@router.websocket("/ws/live/{game_id}")
async def websocket_live_game_endpoint(websocket: WebSocket, game_id: str) -> None:
    """WebSocket endpoint for subscribing to real-time play-by-play events and WPA updates."""
    await ws_manager.connect(websocket, game_id)
    try:
        await websocket.send_json(
            {
                "type": StreamEventType.CONNECTION_ESTABLISHED.value,
                "game_id": game_id,
                "active_viewers": ws_manager.get_active_count(game_id),
                "circuit_state": live_relay_breaker.state.value,
                "message": f"Subscribed to real-time stream for game {game_id}",
            }
        )

        while True:
            data = await websocket.receive_text()
            if data.lower() == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, game_id)
        logger.info("Client disconnected from WebSocket stream: %s", game_id)
    except Exception as e:  # noqa: BLE001
        ws_manager.disconnect(websocket, game_id)
        logger.warning("WebSocket connection exception for %s: %s", game_id, e)


async def _run_simulation_task(game_id: str, req: SimulateGameRequest) -> None:
    """Execute asynchronous simulation worker and push events to WebSocket channel."""
    generator = GameStreamGenerator(
        home_team=req.home_team,
        away_team=req.away_team,
        seed=req.seed,
    )
    events = generator.generate_game_stream(game_id=game_id, max_innings=req.innings)

    for ev in events:
        payload = {
            "type": StreamEventType.PLAY_EVENT.value,
            "game_id": game_id,
            "event": ev.to_dict(),
        }
        await ws_manager.broadcast_to_game(game_id, payload)
        if req.speed > 0:
            await asyncio.sleep(max(0.001, 0.05 / req.speed))

    final_payload = {
        "type": StreamEventType.GAME_FINISHED.value,
        "game_id": game_id,
        "summary": {
            "game_id": game_id,
            "home_team": req.home_team,
            "away_team": req.away_team,
            "total_events": len(events),
        },
    }
    await ws_manager.broadcast_to_game(game_id, final_payload)


@router.post(
    "/api/v1/live/{game_id}/simulate",
    summary="Trigger interactive live game simulation stream",
    description="Spawns a background simulation generating synthetic play events broadcast via WebSocket.",
)
async def trigger_live_simulation(
    game_id: str,
    req: SimulateGameRequest,
    background_tasks: BackgroundTasks,
) -> dict[str, Any]:
    """Trigger background game simulation protected by circuit breaker."""
    current_state = await live_relay_breaker.get_state()
    if current_state == CircuitState.OPEN:
        status = live_relay_breaker.get_status()
        detail_msg = (
            f"Live stream service is currently OPEN (degraded). "
            f"Cooldown remaining. Reason: {status.last_failure_reason}"
        )
        raise HTTPException(
            status_code=503,
            detail=detail_msg,
        )

    background_tasks.add_task(_run_simulation_task, game_id, req)
    return {
        "status": "SIMULATION_TRIGGERED",
        "game_id": game_id,
        "home_team": req.home_team,
        "away_team": req.away_team,
        "innings": req.innings,
        "speed": req.speed,
        "websocket_url": f"/ws/live/{game_id}",
    }


@router.get(
    "/api/v1/live/stats",
    response_model=LiveStreamStatsResponse,
    summary="Get active WebSocket stream statistics",
)
def get_live_stream_stats() -> LiveStreamStatsResponse:
    """Return aggregated live WebSocket streaming statistics and circuit status."""
    active_counts = ws_manager.get_all_active_counts()
    global_count = active_counts.pop("_global", 0)
    circuit_stat = live_relay_breaker.get_status()

    return LiveStreamStatsResponse(
        active_global_clients=global_count,
        total_active_channels=len(active_counts),
        channels=active_counts,
        circuit_breakers=[circuit_stat],
    )


@router.get(
    "/api/v1/live/channels",
    response_model=list[LiveChannelStatsDTO],
    summary="List all active game streaming channels and viewer counts",
)
def get_live_channels() -> list[LiveChannelStatsDTO]:
    """Return active game stream channels."""
    return ws_manager.get_channel_stats()


@router.get(
    "/api/v1/live/circuit-status",
    response_model=list[CircuitStatusDTO],
    summary="Inspect live stream circuit breaker telemetry and state",
)
def get_circuit_status() -> list[CircuitStatusDTO]:
    """Return telemetry metrics for all live streaming circuit breakers."""
    return [live_relay_breaker.get_status()]


@router.post(
    "/api/v1/live/circuit/reset",
    response_model=CircuitActionResponse,
    summary="Manually reset circuit breaker to CLOSED state",
)
async def reset_circuit() -> CircuitActionResponse:
    """Manually restore circuit breaker to CLOSED operational state."""
    prev_state = live_relay_breaker.state
    new_state = await live_relay_breaker.reset()
    return CircuitActionResponse(
        status="RESET_SUCCESS",
        circuit_name=live_relay_breaker.name,
        previous_state=prev_state,
        new_state=new_state,
        message=f"Circuit breaker '{live_relay_breaker.name}' successfully reset to {new_state.value}",
    )


@router.post(
    "/api/v1/live/circuit/trip",
    response_model=CircuitActionResponse,
    summary="Manually trip circuit breaker to OPEN state (Admin / Isolation)",
)
async def trip_circuit(req: ManualTripRequest | None = None) -> CircuitActionResponse:
    """Manually trip circuit breaker to OPEN state for fault isolation or testing."""
    reason = req.reason if req else "Manual trip"
    prev_state = live_relay_breaker.state
    new_state = await live_relay_breaker.trip(reason=reason)
    return CircuitActionResponse(
        status="TRIPPED_SUCCESS",
        circuit_name=live_relay_breaker.name,
        previous_state=prev_state,
        new_state=new_state,
        message=f"Circuit breaker '{live_relay_breaker.name}' tripped to {new_state.value}: {reason}",
    )


__all__ = ["live_relay_breaker", "router"]
