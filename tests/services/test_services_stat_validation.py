"""Tests for services with low coverage."""

from __future__ import annotations

import pytest

from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator
from src.utils.player_season_stat_validation import (
    normalize_season_stat_payload,
    validate_season_stat_payload,
)


class TestBattingStatCalculator:
    @pytest.mark.parametrize(
        "data,expected_avg",
        [
            ({"at_bats": 10, "hits": 3}, 0.3),
            ({"at_bats": 100, "hits": 25}, 0.25),
            ({"at_bats": 0, "hits": 0}, 0.0),
        ],
    )
    def test_avg(self, data, expected_avg):
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["avg"] == expected_avg


class TestPitchingStatCalculator:
    @pytest.mark.parametrize(
        "data,expected_era",
        [
            ({"innings_outs": 27, "earned_runs": 3}, 3.0),
            ({"innings_outs": 60, "earned_runs": 5}, 2.25),
            ({"innings_outs": 0, "earned_runs": 0}, 0.0),
        ],
    )
    def test_era(self, data, expected_era):
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == expected_era

    @pytest.mark.parametrize(
        "data,expected_k9",
        [
            ({"innings_outs": 27, "strikeouts": 18}, 18.0),
            ({"innings_outs": 9, "strikeouts": 9}, 27.0),
        ],
    )
    def test_k_per_9(self, data, expected_k9):
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["k_per_nine"] == expected_k9

    @pytest.mark.parametrize(
        "data,expected_whip",
        [
            ({"walks_allowed": 5, "hits_allowed": 10, "innings_outs": 27}, 1.67),
        ],
    )
    def test_whip(self, data, expected_whip):
        result = PitchingStatCalculator.calculate_ratios(data)
        assert abs(result["whip"] - expected_whip) < 0.01


class TestValidateSeasonStatPayload:
    def test_empty_invalid(self):
        is_valid, reason = validate_season_stat_payload({}, stat_type="batting")
        assert is_valid is False
        assert reason is not None

    def test_valid_batting(self):
        payload = {
            "player_id": 1,
            "name": "Test",
            "team_code": "LG",
            "season": 2026,
            "games": 10,
            "plate_appearances": 30,
            "at_bats": 25,
            "hits": 8,
        }
        is_valid, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert is_valid is True
        assert reason is None

    def test_missing_player_id(self):
        payload = {"season": 2026, "games": 10}
        is_valid, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert is_valid is False


class TestNormalizeSeasonStatPayload:
    def test_basic(self):
        result = normalize_season_stat_payload({"player_id": 1, "season": 2026, "name": "Test"})
        assert result["player_id"] == 1
        assert result["season"] == 2026
