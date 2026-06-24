"""Tests for relay_validation — pure validation functions."""

from __future__ import annotations

import pytest

from src.utils.relay_validation import (
    ALL_VALIDATION_STATES,
    TERMINAL_VALIDATION_STATES,
    VALIDATION_PENDING_LIVE,
    VALIDATION_RECOVERED,
    VALIDATION_SOURCE_INCOMPLETE,
    VALIDATION_SOURCE_UNAVAILABLE,
    VALIDATION_UNVERIFIED,
    VALIDATION_VERIFIED,
    _event_sequence_warnings,
    _inning_regression_warnings,
    _last_pbp_score,
    _out_count_warnings,
    _score_regression_warnings,
    _validate_pbp_final_score,
    _validate_pbp_innings,
    validate_live_events,
)


class TestConstants:
    def test_all_states_count(self) -> None:
        assert len(ALL_VALIDATION_STATES) == 7

    def test_terminal_states_subset(self) -> None:
        assert TERMINAL_VALIDATION_STATES <= ALL_VALIDATION_STATES

    def test_terminal_contains_expected(self) -> None:
        assert VALIDATION_VERIFIED in TERMINAL_VALIDATION_STATES
        assert VALIDATION_RECOVERED in TERMINAL_VALIDATION_STATES
        assert VALIDATION_SOURCE_UNAVAILABLE in TERMINAL_VALIDATION_STATES


class TestValidateLiveEvents:
    def test_empty_events(self) -> None:
        assert validate_live_events([]) == []

    def test_single_event_no_warnings(self) -> None:
        events = [{"inning": 1, "inning_half": "top", "outs": 1, "home_score": 0, "away_score": 0}]
        assert validate_live_events(events) == []

    def test_score_regression_detected(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "outs": 1, "home_score": 2, "away_score": 0},
            {"inning": 1, "inning_half": "top", "outs": 2, "home_score": 1, "away_score": 0},
        ]
        warnings = validate_live_events(events)
        assert any("home_score decreased" in w for w in warnings)

    def test_away_score_regression_detected(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "outs": 1, "home_score": 0, "away_score": 3},
            {"inning": 1, "inning_half": "top", "outs": 2, "home_score": 0, "away_score": 1},
        ]
        warnings = validate_live_events(events)
        assert any("away_score decreased" in w for w in warnings)

    def test_inning_regression_detected(self) -> None:
        events = [
            {"inning": 2, "inning_half": "top", "outs": 1, "home_score": 0, "away_score": 0},
            {"inning": 1, "inning_half": "top", "outs": 1, "home_score": 0, "away_score": 0},
        ]
        warnings = validate_live_events(events)
        assert any("inning regressed" in w for w in warnings)

    def test_half_regression_detected(self) -> None:
        events = [
            {"inning": 1, "inning_half": "bottom", "outs": 1, "home_score": 0, "away_score": 0},
            {"inning": 1, "inning_half": "top", "outs": 1, "home_score": 0, "away_score": 0},
        ]
        warnings = validate_live_events(events)
        assert any("half regressed" in w for w in warnings)

    def test_outs_decrease_without_inning_change(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "outs": 2, "home_score": 0, "away_score": 0},
            {"inning": 1, "inning_half": "top", "outs": 1, "home_score": 0, "away_score": 0},
        ]
        warnings = validate_live_events(events)
        assert any("outs decreased" in w for w in warnings)

    def test_outs_max_no_jump_warning(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "outs": 0, "home_score": 0, "away_score": 0},
            {"inning": 1, "inning_half": "top", "outs": 3, "home_score": 0, "away_score": 0},
        ]
        warnings = validate_live_events(events)
        assert warnings == []

    def test_event_seq_reversed(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "outs": 1, "event_seq": 5, "home_score": 0, "away_score": 0},
            {"inning": 1, "inning_half": "top", "outs": 2, "event_seq": 3, "home_score": 0, "away_score": 0},
        ]
        warnings = validate_live_events(events)
        assert any("event_seq reversed" in w for w in warnings)

    def test_clean_sequence_no_warnings(self) -> None:
        events = [
            {"inning": 1, "inning_half": "top", "outs": 0, "event_seq": 1, "home_score": 0, "away_score": 0},
            {"inning": 1, "inning_half": "top", "outs": 1, "event_seq": 2, "home_score": 0, "away_score": 1},
            {"inning": 1, "inning_half": "top", "outs": 2, "event_seq": 3, "home_score": 0, "away_score": 1},
            {"inning": 1, "inning_half": "bottom", "outs": 0, "event_seq": 4, "home_score": 2, "away_score": 1},
        ]
        assert validate_live_events(events) == []


class TestScoreRegressionWarnings:
    def test_no_regression(self) -> None:
        result = _score_regression_warnings(1, 5, 3, 3, 2)
        assert result == []

    def test_home_regression(self) -> None:
        result = _score_regression_warnings(1, 2, 3, 5, 2)
        assert len(result) == 1
        assert "home_score decreased" in result[0]

    def test_away_regression(self) -> None:
        result = _score_regression_warnings(1, 5, 1, 5, 3)
        assert len(result) == 1
        assert "away_score decreased" in result[0]

    def test_first_event_no_warnings(self) -> None:
        assert _score_regression_warnings(0, 0, 0, 0, 0) == []


class TestInningRegressionWarnings:
    def test_first_event_no_warnings(self) -> None:
        assert _inning_regression_warnings(0, 1, "top", None, None) == []

    def test_inning_progression_ok(self) -> None:
        assert _inning_regression_warnings(1, 2, "top", 1, "bottom") == []

    def test_inning_regression(self) -> None:
        result = _inning_regression_warnings(1, 1, "top", 2, "bottom")
        assert len(result) == 1
        assert "inning regressed" in result[0]

    def test_half_regression(self) -> None:
        result = _inning_regression_warnings(1, 1, "top", 1, "bottom")
        assert len(result) == 1
        assert "half regressed" in result[0]

    def test_same_inning_same_half_ok(self) -> None:
        assert _inning_regression_warnings(1, 1, "top", 1, "top") == []


class TestOutCountWarnings:
    def test_no_outs_no_warnings(self) -> None:
        assert _out_count_warnings(1, None, 1, "top", 0, 1, "top") == []

    def test_first_event_no_warnings(self) -> None:
        assert _out_count_warnings(0, 1, 1, "top", 0, None, None) == []

    def test_out_of_range(self) -> None:
        result = _out_count_warnings(1, -1, 1, "top", 0, 1, "top")
        assert len(result) == 1
        assert "out of range" in result[0]

    def test_out_of_range_high(self) -> None:
        from src.constants import MAX_OUTS

        result = _out_count_warnings(1, MAX_OUTS + 1, 1, "top", 0, 1, "top")
        assert len(result) == 1

    def test_different_inning_no_warning(self) -> None:
        assert _out_count_warnings(1, 1, 2, "top", 0, 1, "top") == []

    def test_different_half_no_warning(self) -> None:
        assert _out_count_warnings(1, 1, 1, "bottom", 0, 1, "top") == []

    def test_normal_progression(self) -> None:
        assert _out_count_warnings(1, 1, 1, "top", 0, 1, "top") == []

    def test_max_outs_no_warning(self) -> None:
        from src.constants import MAX_OUTS

        result = _out_count_warnings(1, MAX_OUTS, 1, "top", 0, 1, "top")
        assert result == []


class TestEventSequenceWarnings:
    def test_first_event_no_warnings(self) -> None:
        assert _event_sequence_warnings(0, {"event_seq": 1}, []) == []

    def test_no_seq_no_warnings(self) -> None:
        assert _event_sequence_warnings(1, {}, [{"event_seq": 1}]) == []

    def test_increasing_ok(self) -> None:
        assert _event_sequence_warnings(1, {"event_seq": 5}, [{"event_seq": 3}]) == []

    def test_reversed_detected(self) -> None:
        result = _event_sequence_warnings(1, {"event_seq": 2}, [{"event_seq": 5}])
        assert len(result) == 1
        assert "event_seq reversed" in result[0]

    def test_equal_detected(self) -> None:
        result = _event_sequence_warnings(1, {"event_seq": 3}, [{"event_seq": 3}])
        assert len(result) == 1


class TestValidatePbpInnings:
    def test_single_inning(self) -> None:
        assert _validate_pbp_innings([], [{"inning": 1}]) is None

    def test_missing_first_inning(self) -> None:
        result = _validate_pbp_innings([], [{"inning": 2}])
        assert result is not None
        assert "starts_at_inning_2" in result

    def test_missing_middle_inning(self) -> None:
        result = _validate_pbp_innings([], [{"inning": 1}, {"inning": 3}])
        assert result is not None
        assert "missing_innings" in result

    def test_uses_events_when_no_rows(self) -> None:
        events = [{"inning": 1}, {"inning": 2}]
        assert _validate_pbp_innings(events, []) is None

    def test_empty_returns_error(self) -> None:
        result = _validate_pbp_innings([], [])
        assert result == "no_innings_found"


class TestLastPbpScore:
    def test_returns_last_scores(self) -> None:
        events = [
            {"home_score": 3, "away_score": 2},
            {"home_score": 5, "away_score": 3},
        ]
        assert _last_pbp_score(events) == (5, 3)

    def test_skips_none_scores(self) -> None:
        events = [
            {"home_score": None, "away_score": None},
            {"home_score": 2, "away_score": 1},
        ]
        assert _last_pbp_score(events) == (2, 1)

    def test_returns_none_for_all_none(self) -> None:
        assert _last_pbp_score([{"home_score": None, "away_score": None}]) is None

    def test_returns_none_for_empty(self) -> None:
        assert _last_pbp_score([]) is None
