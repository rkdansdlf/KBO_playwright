"""Integration tests for Analytics API Router."""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from src.analytics.dto import (
    BattingSabermetrics,
    LeagueConstants,
    MatchupMatrix,
    PitchingSabermetrics,
    SplitMetrics,
)
from src.api.app import app
from src.api.auth import get_api_key
from src.api.dependencies import get_matchup_engine, get_sabermetrics_engine

client = TestClient(app)


def test_get_league_constants() -> None:
    mock_engine = MagicMock()
    mock_engine.get_league_constants.return_value = LeagueConstants(
        year=2025,
        level="KBO1",
        woba_scale=1.25,
        league_woba=0.330,
        league_era=4.20,
        fip_constant=3.80,
    )

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_sabermetrics_engine] = lambda: mock_engine

    try:
        response = client.get("/api/analytics/constants?year=2025&level=KBO1")
        assert response.status_code == 200
        data = response.json()
        assert data["year"] == 2025
        assert data["level"] == "KBO1"
        assert data["league_woba"] == 0.330
    finally:
        app.dependency_overrides.clear()


def test_get_batting_sabermetrics() -> None:
    mock_engine = MagicMock()
    mock_engine.get_league_constants.return_value = LeagueConstants(year=2025)
    mock_engine.session.query.return_value.filter.return_value.all.return_value = [MagicMock()]
    mock_engine.calculate_batting_metrics.return_value = BattingSabermetrics(
        player_id=78224,
        season=2025,
        woba=0.395,
        wrc_plus=138.4,
        war=4.85,
    )

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_sabermetrics_engine] = lambda: mock_engine

    try:
        response = client.get("/api/analytics/batting?year=2025")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["player_id"] == 78224
        assert data[0]["woba"] == 0.395
    finally:
        app.dependency_overrides.clear()


def test_get_pitching_sabermetrics() -> None:
    mock_engine = MagicMock()
    mock_engine.get_league_constants.return_value = LeagueConstants(year=2025)
    mock_engine.session.query.return_value.filter.return_value.all.return_value = [MagicMock()]
    mock_engine.calculate_pitching_metrics.return_value = PitchingSabermetrics(
        player_id=61234,
        season=2025,
        era=3.0,
        fip=3.25,
        war=4.20,
    )

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_sabermetrics_engine] = lambda: mock_engine

    try:
        response = client.get("/api/analytics/pitching?year=2025")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["player_id"] == 61234
        assert data[0]["fip"] == 3.25
    finally:
        app.dependency_overrides.clear()


def test_get_bvp_matchups() -> None:
    mock_matchup = MagicMock()
    mock_matchup.calculate_bvp_matchups.return_value = [
        MatchupMatrix(
            batter_id=78224,
            pitcher_id=61234,
            plate_appearances=15,
            hits=4,
            avg=0.333,
        )
    ]

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_matchup_engine] = lambda: mock_matchup

    try:
        response = client.get("/api/analytics/matchup/bvp?year=2025")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["batter_id"] == 78224
        assert data[0]["pitcher_id"] == 61234
    finally:
        app.dependency_overrides.clear()


def test_get_risp_splits() -> None:
    mock_matchup = MagicMock()
    mock_matchup.calculate_situational_splits.return_value = [
        SplitMetrics(
            category="risp",
            entity_id=78224,
            season=2025,
            split_key="RISP",
            sample_size=110,
            stats={"avg": 0.356, "rbi": 45},
        )
    ]

    app.dependency_overrides[get_api_key] = lambda: "test_key"
    app.dependency_overrides[get_matchup_engine] = lambda: mock_matchup

    try:
        response = client.get("/api/analytics/splits/risp?year=2025")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["split_key"] == "RISP"
        assert data[0]["stats"]["rbi"] == 45
    finally:
        app.dependency_overrides.clear()
