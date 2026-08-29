"""Integration tests for Live Stream Router, multi-channel WebSocket broker, and circuit breaker endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api.app import app
from src.api.live_stream_dto import CircuitState, StreamEventType
from src.api.routers.live_stream import live_relay_breaker

client = TestClient(app)


@pytest.fixture(autouse=True)
def _reset_global_breaker() -> None:
    """Ensure live relay circuit breaker is in CLOSED state before each test."""
    import asyncio

    asyncio.run(live_relay_breaker.reset())


def test_websocket_single_game_stream() -> None:
    """Test single game WebSocket handshake, viewer count, and ping/pong."""
    game_id = "20260501KIAHH0"
    with client.websocket_connect(f"/ws/live/{game_id}") as ws:
        data = ws.receive_json()
        assert data["type"] == StreamEventType.CONNECTION_ESTABLISHED.value
        assert data["game_id"] == game_id
        assert data["active_viewers"] >= 1
        assert "circuit_state" in data

        ws.send_text("ping")
        assert ws.receive_text() == "pong"


def test_websocket_multi_game_broker() -> None:
    """Test multi-game dynamic subscription, unsubscription, and error handling."""
    with client.websocket_connect("/ws/live/multi") as ws:
        init_data = ws.receive_json()
        assert init_data["type"] == StreamEventType.CONNECTION_ESTABLISHED.value

        # Subscribe to two games
        ws.send_json({"action": "subscribe", "games": ["20260501LGSS0", "20260501NCKT0"]})
        sub_data = ws.receive_json()
        assert sub_data["type"] == StreamEventType.CHANNEL_SUBSCRIBED.value
        assert "20260501LGSS0" in sub_data["subscribed_games"]
        assert "20260501NCKT0" in sub_data["subscribed_games"]

        # Unsubscribe from one game
        ws.send_json({"action": "unsubscribe", "games": ["20260501LGSS0"]})
        unsub_data = ws.receive_json()
        assert unsub_data["type"] == StreamEventType.CHANNEL_UNSUBSCRIBED.value
        assert "20260501LGSS0" in unsub_data["unsubscribed_games"]

        # Send invalid action
        ws.send_json({"action": "unknown_action"})
        err_data = ws.receive_json()
        assert err_data["type"] == StreamEventType.ERROR.value


def test_get_live_stream_stats_and_channels() -> None:
    """Test live stream statistics and channel listing REST APIs."""
    # Stats
    resp = client.get("/api/v1/live/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "active_global_clients" in data
    assert "total_active_channels" in data
    assert "circuit_breakers" in data
    assert len(data["circuit_breakers"]) >= 1

    # Channels
    resp_ch = client.get("/api/v1/live/channels")
    assert resp_ch.status_code == 200
    assert isinstance(resp_ch.json(), list)


def test_circuit_breaker_admin_trip_and_reset_endpoints() -> None:
    """Test REST endpoints for inspecting, tripping, and resetting circuit breaker."""
    # 1. Inspect status
    resp = client.get("/api/v1/live/circuit-status")
    assert resp.status_code == 200
    statuses = resp.json()
    assert len(statuses) >= 1
    assert statuses[0]["state"] == CircuitState.CLOSED.value

    # 2. Manual trip
    resp_trip = client.post("/api/v1/live/circuit/trip", json={"reason": "Emergency Maintenance Test"})
    assert resp_trip.status_code == 200
    trip_data = resp_trip.json()
    assert trip_data["status"] == "TRIPPED_SUCCESS"
    assert trip_data["new_state"] == CircuitState.OPEN.value

    # Check simulation is now blocked (503 Service Unavailable)
    resp_sim = client.post(
        "/api/v1/live/20260501LGSS0/simulate",
        json={"home_team": "KIA", "away_team": "LG", "innings": 1, "speed": 0.0},
    )
    assert resp_sim.status_code == 503
    assert "currently OPEN" in resp_sim.json()["detail"]

    # 3. Manual reset
    resp_reset = client.post("/api/v1/live/circuit/reset")
    assert resp_reset.status_code == 200
    reset_data = resp_reset.json()
    assert reset_data["status"] == "RESET_SUCCESS"
    assert reset_data["new_state"] == CircuitState.CLOSED.value

    # Check simulation is now accepted (200 OK)
    resp_sim_ok = client.post(
        "/api/v1/live/20260501LGSS0/simulate",
        json={"home_team": "KIA", "away_team": "LG", "innings": 1, "speed": 0.0},
    )
    assert resp_sim_ok.status_code == 200
    assert resp_sim_ok.json()["status"] == "SIMULATION_TRIGGERED"
