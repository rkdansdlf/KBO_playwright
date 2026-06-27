"""Tests for wpa_transitions pure functions."""

from __future__ import annotations

import pytest

from src.services.wpa_transitions import (
    get_event_value,
    format_base_string,
    parse_base_string,
    coerce_int,
    event_runner_state,
    event_has_transition_state,
    event_has_wpa_state,
    apply_wpa_transitions,
)


class TestGetEventValue:
    def test_dict_access(self) -> None:
        assert get_event_value({"key": "val"}, "key") == "val"

    def test_dict_missing(self) -> None:
        assert get_event_value({"key": "val"}, "other") is None

    def test_object_attribute(self) -> None:
        class Obj:
            x = 42

        assert get_event_value(Obj(), "x") == 42

    def test_object_missing_attr(self) -> None:
        class Obj:
            pass

        assert get_event_value(Obj(), "missing") is None


class TestFormatBaseString:
    def test_all_on(self) -> None:
        assert format_base_string(7) == "123"

    def test_all_off(self) -> None:
        assert format_base_string(0) == "---"

    def test_first_only(self) -> None:
        assert format_base_string(1) == "1--"

    def test_second_only(self) -> None:
        assert format_base_string(2) == "-2-"

    def test_third_only(self) -> None:
        assert format_base_string(4) == "--3"

    def test_none(self) -> None:
        assert format_base_string(None) == "---"

    def test_first_and_third(self) -> None:
        assert format_base_string(5) == "1-3"


class TestParseBaseString:
    def test_all_on(self) -> None:
        assert parse_base_string("123") == 7

    def test_all_off(self) -> None:
        assert parse_base_string("---") == 0

    def test_first_only(self) -> None:
        assert parse_base_string("1--") == 1

    def test_none(self) -> None:
        assert parse_base_string(None) is None

    def test_empty(self) -> None:
        assert parse_base_string("") is None

    def test_whitespace(self) -> None:
        assert parse_base_string("   ") is None

    def test_with_zeros(self) -> None:
        assert parse_base_string("100") == 1

    def test_mixed(self) -> None:
        assert parse_base_string("-2-") == 2

    def test_single_char(self) -> None:
        assert parse_base_string("1") == 1


class TestCoerceInt:
    def test_none(self) -> None:
        assert coerce_int(None) is None

    def test_empty(self) -> None:
        assert coerce_int("") is None

    def test_valid_int(self) -> None:
        assert coerce_int("5") == 5

    def test_int_value(self) -> None:
        assert coerce_int(42) == 42

    def test_invalid_string(self) -> None:
        assert coerce_int("abc") is None


class TestEventRunnerState:
    def test_with_base_state(self) -> None:
        class Event:
            base_state = 5

        assert event_runner_state(Event()) == 5

    def test_with_bases_after(self) -> None:
        class Event:
            bases_after = "1-3"

        assert event_runner_state(Event()) == 5

    def test_with_bases_before(self) -> None:
        class Event:
            bases_before = "12-"

        assert event_runner_state(Event()) == 3

    def test_no_runners(self) -> None:
        class Event:
            pass

        assert event_runner_state(Event()) is None

    def test_none_base_state_falls_through(self) -> None:
        class Event:
            base_state = None
            bases_after = "1--"

        assert event_runner_state(Event()) == 1


class TestEventHasTransitionState:
    def test_true(self) -> None:
        class Event:
            inning = 5
            inning_half = "top"
            outs = 2
            description = "Single"
            home_score = 1
            away_score = 0
            base_state = 3

        assert event_has_transition_state(Event()) is True

    def test_false_no_inning(self) -> None:
        class Event:
            inning = None
            inning_half = "top"
            outs = 2
            description = "Single"
            home_score = 1
            away_score = 0
            base_state = 3

        assert event_has_transition_state(Event()) is False

    def test_false_wrong_half(self) -> None:
        class Event:
            inning = 5
            inning_half = "middle"
            outs = 2
            description = "Single"
            home_score = 1
            away_score = 0
            base_state = 3

        assert event_has_transition_state(Event()) is False

    def test_no_attribute(self) -> None:
        class Event:
            pass

        assert event_has_transition_state(Event()) is False


class TestEventHasWpaState:
    def test_true(self) -> None:
        class Event:
            inning = 5
            inning_half = "top"
            outs = 2
            description = "Single"
            home_score = 1
            away_score = 0
            base_state = 3
            wpa = 0.5
            win_expectancy_before = 0.4
            win_expectancy_after = 0.6

        assert event_has_wpa_state(Event()) is True

    def test_false_no_wpa(self) -> None:
        class Event:
            inning = 5
            inning_half = "top"
            outs = 2
            description = "Single"
            home_score = 1
            away_score = 0
            base_state = 3
            wpa = None
            win_expectancy_before = 0.4
            win_expectancy_after = 0.6

        assert event_has_wpa_state(Event()) is False


class TestApplyWpaTransitions:
    def test_empty_list(self) -> None:
        result = apply_wpa_transitions([])
        assert result is None

    def test_dict_event_full_state(self) -> None:
        events = [
            {
                "inning": 5,
                "inning_half": "top",
                "outs": 2,
                "description": "Single",
                "home_score": 1,
                "away_score": 0,
                "base_state": 3,
            }
        ]
        apply_wpa_transitions(events)
        assert "bases_before" in events[0]
        assert "bases_after" in events[0]
        assert "win_expectancy_before" in events[0]
        assert "win_expectancy_after" in events[0]
