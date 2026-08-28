"""Tests for FastAPI real-time WebSocket live game streaming endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.api.app import app
from src.api.websocket_manager import ConnectionManager

client = TestClient(app)


def test_websocket_live_game_handshake() -> None:
    """Test connecting to live game WebSocket channel and receiving handshake."""
    game_id = "20260401LGHT0"
    with client.websocket_connect(f"/ws/live/{game_id}") as websocket:
        data = websocket.receive_json()
        assert data["type"] == "CONNECTION_ESTABLISHED"
        assert data["game_id"] == game_id
        assert "active_viewers" in data

        # Test ping / pong communication
        websocket.send_text("ping")
        resp = websocket.receive_text()
        assert resp == "pong"


def test_trigger_live_simulation_endpoint() -> None:
    """Test triggering game simulation via REST API."""
    game_id = "20260401LGHT0"
    response = client.post(
        f"/api/v1/live/{game_id}/simulate",
        json={"home_team": "KIA", "away_team": "LG", "innings": 2, "speed": 0.0, "seed": 42},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "SIMULATION_TRIGGERED"
    assert data["game_id"] == game_id
    assert data["websocket_url"] == f"/ws/live/{game_id}"


def test_get_live_stream_stats_endpoint() -> None:
    """Test getting active stream connection statistics."""
    response = client.get("/api/v1/live/stats")
    assert response.status_code == 200
    data = response.json()
    assert "active_global_clients" in data
    assert "channels" in data


def test_connection_manager_unit_lifecycle() -> None:
    """Test ConnectionManager internal registration and tracking."""
    cm = ConnectionManager()
    assert cm.get_active_count("test_game") == 0
    assert cm.get_active_count() == 0

    counts = cm.get_all_active_counts()
    assert counts["_global"] == 0
