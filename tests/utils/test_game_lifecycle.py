from __future__ import annotations

from src.utils.game_state import (
    ALLOWED_TRANSITIONS,
    LIFECYCLE_STATES,
    RELAY_ACTIVE_STATES,
    TERMINAL_STATES,
    GameLifecycleState,
    derive_lifecycle_from_naver_status,
    is_terminal,
    validate_transition,
)


class TestLifecycleStates:
    def test_all_states_defined(self):
        assert "before" in LIFECYCLE_STATES
        assert "running" in LIFECYCLE_STATES
        assert "delayed" in LIFECYCLE_STATES
        assert "suspended" in LIFECYCLE_STATES
        assert "cancelled" in LIFECYCLE_STATES
        assert "result_pending_stabilization" in LIFECYCLE_STATES
        assert "final" in LIFECYCLE_STATES

    def test_terminal_states(self):
        assert {"cancelled", "final"} == TERMINAL_STATES

    def test_relay_active_states(self):
        assert {"running", "delayed", "suspended", "result_pending_stabilization"} == RELAY_ACTIVE_STATES


class TestIsTerminal:
    def test_cancelled_is_terminal(self):
        assert is_terminal("cancelled") is True

    def test_final_is_terminal(self):
        assert is_terminal("final") is True

    def test_running_is_not_terminal(self):
        assert is_terminal("running") is False

    def test_before_is_not_terminal(self):
        assert is_terminal("before") is False


class TestValidateTransition:
    def test_valid_before_to_running(self):
        valid, err = validate_transition(None, "running")
        assert valid is True
        assert err is None

    def test_valid_before_to_cancelled(self):
        valid, err = validate_transition("before", "cancelled")
        assert valid is True

    def test_valid_running_to_delayed(self):
        valid, err = validate_transition("running", "delayed")
        assert valid is True

    def test_valid_running_to_final(self):
        valid, err = validate_transition("running", "result_pending_stabilization")
        assert valid is True

    def test_invalid_terminal_to_running(self):
        valid, err = validate_transition("cancelled", "running")
        assert valid is False
        assert err is not None

    def test_invalid_unknown_state(self):
        valid, err = validate_transition(None, "unknown_state")
        assert valid is False
        assert "unknown" in err

    def test_none_current_is_always_valid(self):
        valid, err = validate_transition(None, "final")
        assert valid is True


class TestDeriveLifecycleFromNaverStatus:
    def test_before(self):
        assert derive_lifecycle_from_naver_status("BEFORE") == "before"

    def test_running(self):
        assert derive_lifecycle_from_naver_status("RUNNING") == "running"

    def test_result(self):
        assert derive_lifecycle_from_naver_status("RESULT") == "result_pending_stabilization"

    def test_cancel(self):
        assert derive_lifecycle_from_naver_status("CANCEL") == "cancelled"

    def test_cancelled(self):
        assert derive_lifecycle_from_naver_status("CANCELLED") == "cancelled"

    def test_delayed(self):
        assert derive_lifecycle_from_naver_status("DELAYED") == "delayed"

    def test_suspended(self):
        assert derive_lifecycle_from_naver_status("SUSPENDED") == "suspended"

    def test_postponed(self):
        assert derive_lifecycle_from_naver_status("POSTPONED") == "cancelled"

    def test_none(self):
        assert derive_lifecycle_from_naver_status(None) is None

    def test_empty_string(self):
        assert derive_lifecycle_from_naver_status("") is None

    def test_lowercase(self):
        assert derive_lifecycle_from_naver_status("before") == "before"


class TestAllowedTransitions:
    def test_before_transitions(self):
        before_targets = {t[1] for t in ALLOWED_TRANSITIONS if t[0] == "before"}
        assert before_targets == {"running", "cancelled"}

    def test_running_transitions(self):
        running_targets = {t[1] for t in ALLOWED_TRANSITIONS if t[0] == "running"}
        assert running_targets == {"delayed", "suspended", "result_pending_stabilization", "cancelled"}

    def test_result_transitions(self):
        result_targets = {t[1] for t in ALLOWED_TRANSITIONS if t[0] == "result_pending_stabilization"}
        assert result_targets == {"final", "running", "delayed", "suspended"}
