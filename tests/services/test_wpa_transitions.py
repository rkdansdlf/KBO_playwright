from __future__ import annotations

from unittest.mock import MagicMock

from src.services.wpa_transitions import (
    apply_wpa_transitions,
    coerce_int,
    event_has_transition_state,
    event_has_wpa_state,
    event_runner_state,
    format_base_string,
    get_event_value,
    parse_base_string,
)


class TestGetEventValue:
    def test_dict_access(self):
        assert get_event_value({"a": 1}, "a") == 1

    def test_dict_missing(self):
        assert get_event_value({"a": 1}, "b") is None

    def test_object_attr(self):
        obj = MagicMock(a=42)
        assert get_event_value(obj, "a") == 42

    def test_object_missing(self):
        obj = MagicMock(spec=[])
        assert get_event_value(obj, "nonexistent") is None

    def test_none_key_dict(self):
        assert get_event_value({"key": None}, "key") is None


class TestFormatBaseString:
    def test_zero(self):
        assert format_base_string(0) == "---"

    def test_first_only(self):
        assert format_base_string(1) == "1--"

    def test_second_only(self):
        assert format_base_string(2) == "-2-"

    def test_third_only(self):
        assert format_base_string(4) == "--3"

    def test_first_and_second(self):
        assert format_base_string(3) == "12-"

    def test_first_and_third(self):
        assert format_base_string(5) == "1-3"

    def test_second_and_third(self):
        assert format_base_string(6) == "-23"

    def test_loaded(self):
        assert format_base_string(7) == "123"

    def test_none(self):
        assert format_base_string(None) == "---"

    def test_unknown_int(self):
        assert format_base_string(99) == "12-"


class TestParseBaseString:
    def test_none(self):
        assert parse_base_string(None) is None

    def test_empty_string(self):
        assert parse_base_string("") is None

    def test_all_dash(self):
        assert parse_base_string("---") == 0

    def test_first(self):
        assert parse_base_string("1--") == 1

    def test_second(self):
        assert parse_base_string("-2-") == 2

    def test_third(self):
        assert parse_base_string("--3") == 4

    def test_full(self):
        assert parse_base_string("123") == 7

    def test_with_zeros(self):
        assert parse_base_string("000") == 0

    def test_shorter_than_three(self):
        assert parse_base_string("12") == 3

    def test_single_char(self):
        assert parse_base_string("1") == 1


class TestCoerceInt:
    def test_none(self):
        assert coerce_int(None) is None

    def test_empty_string(self):
        assert coerce_int("") is None

    def test_valid_int(self):
        assert coerce_int(42) == 42

    def test_valid_str(self):
        assert coerce_int("42") == 42

    def test_invalid_str(self):
        assert coerce_int("abc") is None

    def test_zero(self):
        assert coerce_int(0) == 0

    def test_float(self):
        assert coerce_int(3.14) == 3


class TestEventRunnerState:
    def test_from_base_state(self):
        event = {"base_state": 3}
        assert event_runner_state(event) == 3

    def test_base_state_none_falls_back_to_bases_after(self):
        event = {"base_state": None, "bases_after": "12-"}
        assert event_runner_state(event) == 3

    def test_all_none(self):
        event = {"base_state": None}
        assert event_runner_state(event) is None

    def test_bases_before(self):
        event = {"base_state": None, "bases_before": "1--"}
        assert event_runner_state(event) == 1

    def test_bases_after_preferred_over_before(self):
        event = {"base_state": None, "bases_after": "-2-", "bases_before": "1--"}
        assert event_runner_state(event) == 2

    def test_base_state_zero(self):
        event = {"base_state": 0}
        assert event_runner_state(event) == 0

    def test_missing_keys(self):
        event = {}
        assert event_runner_state(event) is None


class TestEventHasTransitionState:
    def test_valid_event(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "outs": 2,
            "description": "fly ball",
            "home_score": 3,
            "away_score": 1,
            "base_state": 0,
        }
        assert event_has_transition_state(event)

    def test_missing_inning(self):
        event = {"inning_half": "top", "outs": 0, "description": "x", "home_score": 0, "away_score": 0, "base_state": 0}
        assert not event_has_transition_state(event)

    def test_empty_description(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "outs": 0,
            "description": "",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
        }
        assert not event_has_transition_state(event)

    def test_blank_description(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "outs": 0,
            "description": "   ",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
        }
        assert not event_has_transition_state(event)

    def test_missing_outs(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "description": "hit",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
        }
        assert not event_has_transition_state(event)

    def test_invalid_inning_half(self):
        event = {
            "inning": 1,
            "inning_half": "middle",
            "outs": 0,
            "description": "x",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
        }
        assert not event_has_transition_state(event)

    def test_missing_scores(self):
        event = {"inning": 1, "inning_half": "top", "outs": 0, "description": "x", "base_state": 0}
        assert not event_has_transition_state(event)

    def test_missing_runner_state(self):
        event = {"inning": 1, "inning_half": "top", "outs": 0, "description": "x", "home_score": 0, "away_score": 0}
        assert not event_has_transition_state(event)


class TestEventHasWpaState:
    def test_valid_event(self):
        event = {
            "inning": 1,
            "inning_half": "bottom",
            "outs": 0,
            "description": "single",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
            "wpa": 0.1,
            "win_expectancy_before": 0.5,
            "win_expectancy_after": 0.6,
        }
        assert event_has_wpa_state(event)

    def test_missing_wpa(self):
        event = {
            "inning": 1,
            "inning_half": "bottom",
            "outs": 0,
            "description": "single",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
            "win_expectancy_before": 0.5,
            "win_expectancy_after": 0.6,
        }
        assert not event_has_wpa_state(event)

    def test_missing_we_before(self):
        event = {
            "inning": 1,
            "inning_half": "bottom",
            "outs": 0,
            "description": "single",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
            "wpa": 0.1,
            "win_expectancy_after": 0.6,
        }
        assert not event_has_wpa_state(event)


class MockWPACalculator:
    def get_win_probability(self, inning, is_bottom, outs, runners, score_diff):
        return 0.5


class TestApplyWpaTransitions:
    def test_skips_invalid_events(self):
        events = [{"description": "event without full state"}]
        apply_wpa_transitions(events, calculator=MockWPACalculator())
        assert "wpa" not in events[0]

    def test_only_missing_skips_events_with_wpa(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "outs": 2,
            "description": "strikeout",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
            "wpa": 0.05,
            "win_expectancy_before": 0.5,
            "win_expectancy_after": 0.55,
        }
        events = [event]
        apply_wpa_transitions(events, calculator=MockWPACalculator(), only_missing=True)
        assert events[0]["wpa"] == 0.05

    def test_sets_wpa_on_missing_event(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "outs": 2,
            "description": "single",
            "home_score": 0,
            "away_score": 0,
            "base_state": 1,
        }
        events = [event]
        apply_wpa_transitions(events, calculator=MockWPACalculator())
        assert "wpa" in events[0]
        assert "win_expectancy_before" in events[0]
        assert "win_expectancy_after" in events[0]

    def test_first_event_uses_zero_priors(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "outs": 0,
            "description": "leadoff",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
        }
        events = [event]
        apply_wpa_transitions(events, calculator=MockWPACalculator())
        assert "bases_before" in event

    def test_uses_previous_event_state(self):
        events = [
            {
                "inning": 1,
                "inning_half": "bottom",
                "outs": 0,
                "description": "walk",
                "home_score": 0,
                "away_score": 0,
                "base_state": 1,
            },
            {
                "inning": 1,
                "inning_half": "bottom",
                "outs": 0,
                "description": "single",
                "home_score": 0,
                "away_score": 0,
                "base_state": 3,
            },
        ]
        apply_wpa_transitions(events, calculator=MockWPACalculator())
        assert "wpa" in events[0]
        assert "wpa" in events[1]

    def test_handles_inning_change(self):
        events = [
            {
                "inning": 1,
                "inning_half": "bottom",
                "outs": 2,
                "description": "out",
                "home_score": 0,
                "away_score": 0,
                "base_state": 0,
            },
            {
                "inning": 2,
                "inning_half": "top",
                "outs": 0,
                "description": "homer",
                "home_score": 0,
                "away_score": 1,
                "base_state": 0,
            },
        ]
        apply_wpa_transitions(events, calculator=MockWPACalculator())
        assert "wpa" in events[0]
        assert "wpa" in events[1]

    def test_no_events(self):
        apply_wpa_transitions([], calculator=MockWPACalculator())

    def test_all_invalid_events(self):
        events = [{"description": ""}, {"description": ""}]
        apply_wpa_transitions(events, calculator=MockWPACalculator())
        assert all("wpa" not in e for e in events)

    def test_wpa_calculation_ties_outcome(self):
        calc = MagicMock()
        calc.get_win_probability.return_value = 0.5
        event = {
            "inning": 1,
            "inning_half": "top",
            "outs": 2,
            "description": "out",
            "home_score": 0,
            "away_score": 0,
            "base_state": 0,
        }
        apply_wpa_transitions([event], calculator=calc)
        assert event["wpa"] == 0.0
        assert event["win_expectancy_before"] == 0.5
        assert event["win_expectancy_after"] == 0.5
