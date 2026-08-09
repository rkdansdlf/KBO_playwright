"""Tests for FastAPI KBO Data API server endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from src.api.app import app
from src.db.engine import init_db


@pytest.fixture(autouse=True)
def _setup_database() -> None:
    """Ensure database tables are initialized before running tests."""
    init_db()


def test_health_check() -> None:
    """Test /health endpoint returns 200 OK."""
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


def test_get_teams_endpoint() -> None:
    """Test /api/teams endpoint returns team list structure."""
    with TestClient(app) as client:
        response = client.get("/api/teams")
        assert response.status_code == 200
        data = response.json()
        assert "teams" in data
        assert isinstance(data["teams"], list)


def test_get_games_endpoint() -> None:
    """Test /api/games endpoint returns game list structure."""
    with TestClient(app) as client:
        response = client.get("/api/games?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert "games" in data
        assert "total" in data
        assert data["limit"] == 5


def test_get_players_endpoint() -> None:
    """Test /api/players endpoint returns player search results."""
    with TestClient(app) as client:
        response = client.get("/api/players?limit=5")
        assert response.status_code == 200
        data = response.json()
        assert "players" in data
        assert "total" in data
        assert data["limit"] == 5


def test_get_standings_endpoint() -> None:
    """Test /api/standings endpoint returns team standings."""
    with TestClient(app) as client:
        response = client.get("/api/standings?season=2025")
        assert response.status_code == 200
        data = response.json()
        assert "standings" in data


def test_run_api_server_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test CLI parser for run_api_server."""
    from src.cli.run_api_server import parse_args

    args = parse_args(["--host", "0.0.0.0", "--port", "9000", "--reload"])
    assert args.host == "0.0.0.0"
    assert args.port == 9000
    assert args.reload is True
