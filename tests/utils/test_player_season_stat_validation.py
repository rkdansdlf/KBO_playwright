from __future__ import annotations

import pytest

from src.utils.player_season_stat_validation import (
    _is_number_like,
    _number_or_none,
    filter_valid_season_stat_payloads,
    normalize_season_stat_payload,
    validate_season_stat_payload,
)


class TestIsNumberLike:
    def test_int(self):
        assert _is_number_like(42) is True

    def test_float(self):
        assert _is_number_like(3.14) is True

    def test_numeric_string(self):
        assert _is_number_like("42") is True

    def test_non_numeric(self):
        assert _is_number_like("abc") is False


class TestNumberOrNone:
    def test_valid_number(self):
        assert _number_or_none("42") == 42.0

    def test_none(self):
        assert _number_or_none(None) is None

    def test_empty(self):
        assert _number_or_none("") is None

    def test_invalid(self):
        assert _number_or_none("abc") is None


class TestValidateSeasonStatPayload:
    def test_empty(self):
        is_valid, reason = validate_season_stat_payload({}, stat_type="batting")
        assert is_valid is False

    def test_minimal_batting(self):
        payload = {
            "player_id": 12345,
            "name": "김하성",
            "team_code": "SS",
            "season": 2026,
            "games": 10,
            "plate_appearances": 40,
            "at_bats": 35,
            "hits": 10,
        }
        is_valid, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert is_valid is True

    def test_missing_player_id(self):
        payload = {
            "season": 2026,
            "games": 10,
        }
        is_valid, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert is_valid is False


class TestNormalizeSeasonStatPayload:
    def test_basic(self):
        payload = {
            "player_id": 12345,
            "season": 2026,
            "games": 10,
        }
        result = normalize_season_stat_payload(payload)
        assert result["player_id"] == 12345
        assert result["season"] == 2026


class TestFilterValidSeasonStatPayloads:
    def test_empty(self):
        valid, invalid = filter_valid_season_stat_payloads([], stat_type="batting")
        assert valid == []

    def test_all_invalid(self):
        payloads = [
            {"player_id": None, "season": 2026},
        ]
        valid, invalid = filter_valid_season_stat_payloads(payloads, stat_type="batting")
        assert len(valid) == 0
