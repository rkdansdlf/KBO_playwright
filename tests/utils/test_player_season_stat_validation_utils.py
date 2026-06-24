from __future__ import annotations

from collections import Counter

from src.utils.player_season_stat_validation import (
    validate_season_stat_payload,
    normalize_season_stat_payload,
    filter_valid_season_stat_payloads,
    _is_number_like,
    _number_or_none,
    _has_core_stats,
    _validate_batting_consistency,
    _validate_pitching_consistency,
)


class TestIsNumberLike:
    def test_none(self):
        assert _is_number_like(None) is True

    def test_int(self):
        assert _is_number_like(42) is True

    def test_float(self):
        assert _is_number_like(3.14) is True

    def test_string_number(self):
        assert _is_number_like("42") is True

    def test_string_comma(self):
        assert _is_number_like("1,234") is True

    def test_string_dash(self):
        assert _is_number_like("-") is True

    def test_string_empty(self):
        assert _is_number_like("") is True

    def test_string_text(self):
        assert _is_number_like("abc") is False


class TestNumberOrNone:
    def test_none(self):
        assert _number_or_none(None) is None

    def test_int(self):
        assert _number_or_none(42) == 42.0

    def test_string_number(self):
        assert _number_or_none("3.14") == 3.14

    def test_string_dash(self):
        assert _number_or_none("-") is None

    def test_string_text(self):
        assert _number_or_none("abc") is None


class TestHasCoreStats:
    def test_batting_has_core(self):
        payload = {"games": 10, "hits": 5}
        assert _has_core_stats(payload, "batting") is True

    def test_batting_empty(self):
        payload = {"player_id": 123}
        assert _has_core_stats(payload, "batting") is False

    def test_pitching_has_core(self):
        payload = {"games": 5, "innings_pitched": 10.0}
        assert _has_core_stats(payload, "pitching") is True

    def test_pitching_empty(self):
        payload = {"player_id": 123}
        assert _has_core_stats(payload, "pitching") is False


class TestValidateBattingConsistency:
    def test_valid(self):
        ok, reason = _validate_batting_consistency({"hits": 5, "at_bats": 20})
        assert ok is True
        assert reason is None

    def test_hits_gt_at_bats(self):
        ok, reason = _validate_batting_consistency({"hits": 25, "at_bats": 20})
        assert ok is False
        assert reason == "hits_gt_at_bats"

    def test_at_bats_gt_plate_appearances(self):
        ok, reason = _validate_batting_consistency({"at_bats": 30, "plate_appearances": 20})
        assert ok is False
        assert reason == "at_bats_gt_plate_appearances"

    def test_empty(self):
        ok, reason = _validate_batting_consistency({})
        assert ok is True


class TestValidatePitchingConsistency:
    def test_valid(self):
        ok, reason = _validate_pitching_consistency({"earned_runs": 3, "runs_allowed": 5})
        assert ok is True
        assert reason is None

    def test_earned_runs_gt_runs_allowed(self):
        ok, reason = _validate_pitching_consistency({"earned_runs": 10, "runs_allowed": 5})
        assert ok is False
        assert reason == "earned_runs_gt_runs_allowed"

    def test_empty(self):
        ok, reason = _validate_pitching_consistency({})
        assert ok is True


class TestValidateSeasonStatPayload:
    def test_valid_batting(self):
        payload = {
            "player_id": 12345,
            "season": 2026,
            "team_code": "LG",
            "player_name": "김선수",
            "games": 10,
            "hits": 5,
        }
        ok, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert ok is True
        assert reason is None

    def test_missing_player_id(self):
        payload = {"season": 2026, "games": 10}
        ok, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert ok is False

    def test_missing_season(self):
        payload = {"player_id": 12345, "team_code": "LG", "player_name": "김선수", "games": 10}
        ok, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert ok is False
        assert reason == "missing_season"

    def test_empty_core_stats(self):
        payload = {"player_id": 12345, "season": 2026, "team_code": "LG", "player_name": "김선수"}
        ok, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert ok is False
        assert reason == "empty_core_stats"

    def test_invalid_numeric(self):
        payload = {
            "player_id": 12345,
            "season": 2026,
            "team_code": "LG",
            "player_name": "김선수",
            "games": "not_a_number",
            "hits": 5,
        }
        ok, reason = validate_season_stat_payload(payload, stat_type="batting")
        assert ok is False
        assert reason == "invalid_numeric_stat"

    def test_valid_pitching(self):
        payload = {
            "player_id": 12345,
            "season": 2026,
            "team_code": "LG",
            "player_name": "김선수",
            "games": 5,
            "innings_pitched": 10.0,
        }
        ok, reason = validate_season_stat_payload(payload, stat_type="pitching")
        assert ok is True


class TestNormalizeSeasonStatPayload:
    def test_normalize_batting(self):
        payload = {"player_id": "12345", "season": "2026", "player_name": "김선수"}
        result = normalize_season_stat_payload(payload)
        assert result["player_id"] == 12345
        assert result["season"] == 2026

    def test_normalize_year_field(self):
        payload = {"player_id": "12345", "year": "2026"}
        result = normalize_season_stat_payload(payload)
        assert result["year"] == 2026


class TestFilterValidSeasonStatPayloads:
    def test_filter_mixed(self):
        payloads = [
            {"player_id": 12345, "season": 2026, "team_code": "LG", "player_name": "김선수", "games": 10, "hits": 5},
            {"player_id": None, "season": 2026, "games": 10},
            {"player_id": 12346, "season": 2026, "team_code": "LG", "player_name": "박선수", "games": 5, "hits": 3},
        ]
        valid, reasons = filter_valid_season_stat_payloads(payloads, stat_type="batting")
        assert len(valid) == 2
        assert isinstance(reasons, Counter)

    def test_filter_empty(self):
        valid, reasons = filter_valid_season_stat_payloads([], stat_type="batting")
        assert valid == []
        assert len(reasons) == 0
