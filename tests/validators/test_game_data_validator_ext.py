"""Tests for game_data_validator."""

from __future__ import annotations

import pytest

from src.validators.game_data_validator import validate_game_data


class TestValidateGameData:
    def test_valid_game_data(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {
                "home": {"code": "LG", "name": "LG 트윈스"},
                "away": {"code": "SSG", "name": "SSG 랜더스"},
            },
            "hitters": {
                "home": [{"name": "김철수"}],
                "away": [{"name": "이영희"}],
            },
            "pitchers": {
                "home": [{"name": "박민수"}],
                "away": [{"name": "최지훈"}],
            },
        }
        is_valid, errors, warnings = validate_game_data(game_data)
        assert is_valid is True
        assert len(errors) == 0

    def test_missing_game_id(self) -> None:
        game_data = {
            "game_date": "2026-06-24",
            "teams": {"home": {"code": "LG"}, "away": {"code": "SSG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert "Missing game_id" in errors

    def test_missing_game_date(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "teams": {"home": {"code": "LG"}, "away": {"code": "SSG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert "Missing game_date" in errors

    def test_missing_team_code(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {"home": {}, "away": {}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert any("home team code" in e for e in errors)
        assert any("away team code" in e for e in errors)

    def test_invalid_team_code(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {"home": {"code": "INVALID"}, "away": {"code": "SSG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert any("Invalid home team code" in e for e in errors)

    def test_missing_hitter_rows(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {"home": {"code": "LG"}, "away": {"code": "SSG"}},
            "hitters": {"home": [], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert any("No hitter rows for home" in e for e in errors)
