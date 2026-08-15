"""Integration tests for Game WPA Chart and Highlights API."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import app

client = TestClient(app)
AUTH_HEADERS = {"X-API-Key": os.getenv("REST_API_KEY", "")}


def test_get_game_wpa_api_success() -> None:
    """Test GET /api/v1/games/{game_id}/wpa with valid data."""
    mock_chart_data = {
        "game_id": "20260809LGKIA0",
        "game_date": "2026-08-09",
        "stadium": "광주",
        "home_team": "KIA",
        "away_team": "LG",
        "home_score": 6,
        "away_score": 3,
        "game_status": "FINAL",
        "timeline": [
            {
                "event_seq": 1,
                "inning": 1,
                "inning_half": "top",
                "batter_name": "홍창기",
                "pitcher_name": "양현종",
                "description": "우전 안타",
                "home_win_prob": 0.47,
                "wpa": 0.03,
                "home_score": 0,
                "away_score": 0,
            }
        ],
        "turning_points": [
            {
                "event_seq": 1,
                "inning": 1,
                "inning_half": "top",
                "description": "우전 안타",
                "batter_name": "홍창기",
                "wpa": 0.03,
                "importance_score": 0.03,
                "impact_type": "MOMENTUM",
            }
        ],
        "home_total_wpa": 0.45,
        "away_total_wpa": -0.45,
    }

    with patch("src.services.wpa_chart_service.WpaChartService.get_game_wpa_chart", return_value=mock_chart_data):
        res = client.get("/api/v1/games/20260809LGKIA0/wpa", headers=AUTH_HEADERS)
        assert res.status_code == 200
        data = res.json()
        assert data["game_id"] == "20260809LGKIA0"
        assert data["home_team"] == "KIA"
        assert len(data["timeline"]) == 1
        assert len(data["turning_points"]) == 1
        assert data["home_total_wpa"] == 0.45


def test_get_game_wpa_api_not_found() -> None:
    """Test GET /api/v1/games/{game_id}/wpa when game not found."""
    with patch("src.services.wpa_chart_service.WpaChartService.get_game_wpa_chart", return_value=None):
        res = client.get("/api/v1/games/NON_EXISTENT/wpa", headers=AUTH_HEADERS)
        assert res.status_code == 404
        assert "not found" in res.json()["detail"].lower()


def test_get_game_highlights_api() -> None:
    """Test GET /api/v1/games/{game_id}/highlights."""
    mock_highlights = [
        {
            "id": 1,
            "game_id": "20260809LGKIA0",
            "event_seq": 42,
            "inning": 7,
            "inning_half": "bottom",
            "highlight_type": "LEAD_CHANGE",
            "description": "김도영 역전 3점 홈런",
            "wpa": 0.385,
            "importance_score": 0.95,
            "tags": ["홈런", "역전"],
        }
    ]

    with patch("src.services.wpa_chart_service.WpaChartService.get_game_highlights", return_value=mock_highlights):
        res = client.get("/api/v1/games/20260809LGKIA0/highlights", headers=AUTH_HEADERS)
        assert res.status_code == 200
        data = res.json()
        assert data["game_id"] == "20260809LGKIA0"
        assert data["count"] == 1
        assert len(data["highlights"]) == 1
        assert data["highlights"][0]["highlight_type"] == "LEAD_CHANGE"
