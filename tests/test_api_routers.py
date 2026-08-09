"""Unit and Integration tests for modularized REST API routers."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api.app import app
from src.db.engine import init_db

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_db() -> None:
    """Ensure database tables are initialized before running API tests."""
    init_db()


def test_health_endpoint() -> None:
    """Test GET /health returns 200 and status ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_status_endpoint() -> None:
    """Test GET /status returns system statistics and lock statuses."""
    response = client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert "database" in data
    assert "locks" in data
    assert "games_count" in data["database"]


def test_healing_status_endpoint() -> None:
    """Test GET /api/v1/health/healing-status returns integrity report."""
    response = client.get("/api/v1/health/healing-status")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "stuck_games_count" in data


def test_get_games_endpoint() -> None:
    """Test GET /api/games returns game list."""
    response = client.get("/api/games?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "games" in data


def test_get_players_endpoint() -> None:
    """Test GET /api/players returns player list."""
    response = client.get("/api/players?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "players" in data


def test_get_teams_endpoint() -> None:
    """Test GET /api/teams returns team list."""
    response = client.get("/api/teams")
    assert response.status_code == 200
    data = response.json()
    assert "teams" in data
