"""
Tests for the game lifecycle state machine (src/utils/game_state.py).
"""

from __future__ import annotations

import pytest

from src.utils.game_state import (
    LIFECYCLE_STATES,
    RELAY_ACTIVE_STATES,
    TERMINAL_STATES,
    derive_lifecycle_from_naver_status,
    validate_transition,
)


class TestValidateTransition:
    def test_none_current_is_valid(self):
        is_valid, reason = validate_transition(None, "running")
        assert is_valid
        assert reason is None

    def test_unknown_lifecycle_state(self):
        is_valid, reason = validate_transition("running", "invalid_state")
        assert not is_valid
        assert reason == "unknown_lifecycle_state_invalid_state"

    def test_unknown_current_state(self):
        is_valid, reason = validate_transition("unknown", "running")
        assert not is_valid
        assert reason == "unknown_current_state_unknown"

    def test_terminal_cancelled_cannot_transition(self):
        is_valid, reason = validate_transition("cancelled", "running")
        assert not is_valid
        assert "terminal_state" in reason

    def test_terminal_final_cannot_transition(self):
        is_valid, reason = validate_transition("final", "running")
        assert not is_valid
        assert "terminal_state" in reason

    def test_valid_before_to_running(self):
        is_valid, reason = validate_transition("before", "running")
        assert is_valid

    def test_valid_running_to_suspended(self):
        is_valid, reason = validate_transition("running", "suspended")
        assert is_valid

    def test_valid_running_to_result(self):
        is_valid, reason = validate_transition("running", "result_pending_stabilization")
        assert is_valid

    def test_valid_result_to_final(self):
        is_valid, reason = validate_transition("result_pending_stabilization", "final")
        assert is_valid

    def test_invalid_before_to_final(self):
        is_valid, reason = validate_transition("before", "final")
        assert not is_valid
        assert "invalid_transition" in reason

    def test_invalid_running_to_before(self):
        is_valid, reason = validate_transition("running", "before")
        assert not is_valid
        assert "invalid_transition" in reason

    @pytest.mark.parametrize("state", LIFECYCLE_STATES)
    def test_all_states_self_transition(self, state):
        """Self-transition is only valid if explicitly in ALLOWED_TRANSITIONS."""
        is_valid, _ = validate_transition(state, state)
        # Most self-transitions are invalid; this just checks no exception is raised
        assert isinstance(is_valid, bool)

    def test_suspended_to_running(self):
        is_valid, reason = validate_transition("suspended", "running")
        assert is_valid, f"suspended -> running should be valid: {reason}"

    def test_suspended_to_cancelled(self):
        is_valid, reason = validate_transition("suspended", "cancelled")
        assert is_valid, f"suspended -> cancelled should be valid: {reason}"


class TestDeriveLifecycleFromNaverStatus:
    def test_before(self):
        assert derive_lifecycle_from_naver_status("BEFORE") == "before"

    def test_running(self):
        assert derive_lifecycle_from_naver_status("RUNNING") == "running"

    def test_result(self):
        assert derive_lifecycle_from_naver_status("RESULT") == "result_pending_stabilization"

    def test_cancel(self):
        assert derive_lifecycle_from_naver_status("CANCEL") == "cancelled"

    def test_lowercase_input(self):
        assert derive_lifecycle_from_naver_status("running") == "running"

    def test_none_input(self):
        assert derive_lifecycle_from_naver_status(None) is None

    def test_empty_input(self):
        assert derive_lifecycle_from_naver_status("") is None

    def test_unknown_status(self):
        assert derive_lifecycle_from_naver_status("UNKNOWN") is None


class TestConstants:
    def test_terminal_states_are_subset(self):
        assert TERMINAL_STATES.issubset(LIFECYCLE_STATES)

    def test_relay_active_states_are_subset(self):
        assert RELAY_ACTIVE_STATES.issubset(LIFECYCLE_STATES)

    def test_cancelled_and_final_are_terminal(self):
        assert "cancelled" in TERMINAL_STATES
        assert "final" in TERMINAL_STATES
        assert len(TERMINAL_STATES) == 2

    def test_result_pending_in_relay_active(self):
        assert "result_pending_stabilization" in RELAY_ACTIVE_STATES
