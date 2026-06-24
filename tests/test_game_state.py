"""Tests for game lifecycle state machine and Naver status mapping."""

from __future__ import annotations

import pytest

from src.utils.game_state import (
    ALLOWED_TRANSITIONS,
    LIFECYCLE_STATES,
    RELAY_ACTIVE_STATES,
    TERMINAL_STATES,
    derive_lifecycle_from_naver_status,
    validate_transition,
)


class TestLifecycleStates:
    def test_all_states_are_valid(self) -> None:
        assert len(LIFECYCLE_STATES) == 7
        assert "before" in LIFECYCLE_STATES
        assert "running" in LIFECYCLE_STATES
        assert "final" in LIFECYCLE_STATES

    def test_terminal_states_subset(self) -> None:
        assert TERMINAL_STATES == {"cancelled", "final"}  # noqa: SIM300
        assert TERMINAL_STATES < LIFECYCLE_STATES

    def test_relay_active_states_subset(self) -> None:
        assert RELAY_ACTIVE_STATES == {"running", "delayed", "suspended", "result_pending_stabilization"}  # noqa: SIM300
        assert RELAY_ACTIVE_STATES < LIFECYCLE_STATES
        assert RELAY_ACTIVE_STATES.isdisjoint(TERMINAL_STATES)


class TestValidateTransition:
    @pytest.mark.parametrize(
        "current,next_state,expected_valid",
        [
            ("before", "running", True),
            ("before", "cancelled", True),
            ("running", "delayed", True),
            ("running", "suspended", True),
            ("running", "cancelled", True),
            ("running", "result_pending_stabilization", True),
            ("result_pending_stabilization", "final", True),
            ("result_pending_stabilization", "running", True),
            ("delayed", "running", True),
            ("suspended", "running", True),
            ("final", "running", False),
            ("cancelled", "running", False),
            ("before", "final", False),
            ("running", "before", False),
        ],
    )
    def test_known_transitions(self, current: str, next_state: str, expected_valid: bool) -> None:
        is_valid, reason = validate_transition(current, next_state)
        assert is_valid == expected_valid

    def test_none_current_is_always_valid(self) -> None:
        is_valid, reason = validate_transition(None, "running")
        assert is_valid is True
        assert reason is None

    def test_unknown_current_state_is_invalid(self) -> None:
        is_valid, reason = validate_transition("unknown_state", "running")
        assert is_valid is False
        assert "unknown_current_state" in reason

    def test_unknown_next_state_is_invalid(self) -> None:
        is_valid, reason = validate_transition("running", "unknown_state")
        assert is_valid is False
        assert "unknown_lifecycle_state" in reason

    @pytest.mark.parametrize(
        "terminal_state",
        ["cancelled", "final"],
    )
    def test_terminal_cannot_transition(self, terminal_state: str) -> None:
        is_valid, reason = validate_transition(terminal_state, "running")
        assert is_valid is False
        assert "terminal_state" in reason


class TestDeriveLifecycleFromNaverStatus:
    @pytest.mark.parametrize(
        "nav_status,expected",
        [
            ("BEFORE", "before"),
            ("RUNNING", "running"),
            ("RESULT", "result_pending_stabilization"),
            ("CANCEL", "cancelled"),
            ("CANCELLED", "cancelled"),
            ("DELAYED", "delayed"),
            ("SUSPENDED", "suspended"),
        ],
    )
    def test_known_statuses(self, nav_status: str, expected: str | None) -> None:
        assert derive_lifecycle_from_naver_status(nav_status) == expected

    def test_none_returns_none(self) -> None:
        assert derive_lifecycle_from_naver_status(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert derive_lifecycle_from_naver_status("") is None

    def test_unknown_status_returns_none(self) -> None:
        assert derive_lifecycle_from_naver_status("UNKNOWN_STATUS") is None

    def test_lowercase_input(self) -> None:
        assert derive_lifecycle_from_naver_status("running") == "running"

    def test_mixed_case_input(self) -> None:
        assert derive_lifecycle_from_naver_status("Running") == "running"

    def test_whitespace_stripped(self) -> None:
        assert derive_lifecycle_from_naver_status("  RUNNING  ") == "running"


class TestAllowedTransitionsSet:
    def test_all_transitions_reference_valid_states(self) -> None:
        for from_s, to_s in ALLOWED_TRANSITIONS:
            assert from_s in LIFECYCLE_STATES or from_s is None, f"Unknown from_state: {from_s}"
            assert to_s in LIFECYCLE_STATES, f"Unknown to_state: {to_s}"

    def test_no_terminal_as_source(self) -> None:
        for from_s, _to_s in ALLOWED_TRANSITIONS:
            assert from_s not in TERMINAL_STATES
