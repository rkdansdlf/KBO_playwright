"""Unit tests for src.testing.dto."""

from __future__ import annotations

from src.testing.dto import (
    SyntheticGameScenario,
    SyntheticGenerationResult,
    SyntheticPlayerScenario,
    SyntheticSeasonConfig,
)


def test_synthetic_player_scenario_to_dict() -> None:
    player = SyntheticPlayerScenario(
        player_id=900001,
        name="LG_타자_1",
        team_code="LG",
        position="내야수",
        is_pitcher=False,
    )
    d = player.to_dict()
    assert d["player_id"] == 900001
    assert d["name"] == "LG_타자_1"
    assert d["team_code"] == "LG"


def test_synthetic_game_scenario_to_dict() -> None:
    game = SyntheticGameScenario(
        game_id="20260401OBSSG0",
        game_date="2026-04-01",
        home_team="SSG",
        away_team="OB",
        home_score=5,
        away_score=3,
    )
    d = game.to_dict()
    assert d["game_id"] == "20260401OBSSG0"
    assert d["home_score"] == 5


def test_synthetic_season_config_to_dict() -> None:
    config = SyntheticSeasonConfig(season_year=2026, games_per_team=3)
    d = config.to_dict()
    assert d["season_year"] == 2026
    assert d["games_per_team"] == 3


def test_synthetic_generation_result_to_dict() -> None:
    result = SyntheticGenerationResult(
        total_games=10,
        total_players=150,
        total_lineups=180,
        total_pbp_events=180,
        elapsed_seconds=0.456,
    )
    d = result.to_dict()
    assert d["total_games"] == 10
    assert d["total_players"] == 150
    assert d["elapsed_seconds"] == 0.456
