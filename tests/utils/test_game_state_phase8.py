from __future__ import annotations

from src.utils.game_state import (
    ALLOWED_TRANSITIONS,
    LIFECYCLE_STATES,
    NAVER_STATUS_MAP,
    RELAY_ACTIVE_STATES,
    TERMINAL_STATES,
    derive_lifecycle_from_naver_status,
    is_terminal,
    validate_transition,
)


class TestLifecycleConstants:
    def test_all_states_count(self):
        assert len(LIFECYCLE_STATES) == 7

    def test_terminal_states_subset(self):
        assert TERMINAL_STATES.issubset(LIFECYCLE_STATES)

    def test_relay_active_subset(self):
        assert RELAY_ACTIVE_STATES.issubset(LIFECYCLE_STATES)

    def test_terminal_not_in_relay_active(self):
        assert TERMINAL_STATES.isdisjoint(RELAY_ACTIVE_STATES)

    def test_all_transitions_within_states(self):
        for from_s, to_s in ALLOWED_TRANSITIONS:
            assert from_s in LIFECYCLE_STATES
            assert to_s in LIFECYCLE_STATES

    def test_no_self_transitions(self):
        for from_s, to_s in ALLOWED_TRANSITIONS:
            assert from_s != to_s


class TestIsTerminalExtended:
    def test_all_terminal_states(self):
        for state in TERMINAL_STATES:
            assert is_terminal(state) is True

    def test_all_non_terminal_states(self):
        for state in LIFECYCLE_STATES - TERMINAL_STATES:
            assert is_terminal(state) is False


class TestValidateTransitionExtended:
    def test_unknown_current_state(self):
        valid, err = validate_transition("invalid_state", "running")
        assert valid is False
        assert "unknown_current_state" in err

    def test_cancelled_to_any_invalid(self):
        for target in LIFECYCLE_STATES - {"cancelled"}:
            valid, err = validate_transition("cancelled", target)
            assert valid is False
            assert "terminal_state" in err

    def test_final_to_any_invalid(self):
        for target in LIFECYCLE_STATES - {"final"}:
            valid, err = validate_transition("final", target)
            assert valid is False
            assert "terminal_state" in err

    def test_before_to_suspended_invalid(self):
        valid, err = validate_transition("before", "suspended")
        assert valid is False
        assert "invalid_transition" in err

    def test_before_to_delayed_invalid(self):
        valid, err = validate_transition("before", "delayed")
        assert valid is False

    def test_before_to_result_pending_invalid(self):
        valid, err = validate_transition("before", "result_pending_stabilization")
        assert valid is False

    def test_running_to_before_invalid(self):
        valid, err = validate_transition("running", "before")
        assert valid is False

    def test_suspended_to_before_invalid(self):
        valid, err = validate_transition("suspended", "before")
        assert valid is False

    def test_result_pending_to_before_invalid(self):
        valid, err = validate_transition("result_pending_stabilization", "before")
        assert valid is False

    def test_result_pending_to_cancelled_invalid(self):
        valid, err = validate_transition("result_pending_stabilization", "cancelled")
        assert valid is False

    def test_all_allowed_transitions_valid(self):
        for from_s, to_s in ALLOWED_TRANSITIONS:
            valid, err = validate_transition(from_s, to_s)
            assert valid is True
            assert err is None

    def test_none_current_to_all_states(self):
        for state in LIFECYCLE_STATES:
            valid, err = validate_transition(None, state)
            assert valid is True
            assert err is None


class TestDeriveLifecycleFromNaverStatusExtended:
    def test_whitespace_padding(self):
        assert derive_lifecycle_from_naver_status("  RUNNING  ") == "running"

    def test_mixed_case(self):
        assert derive_lifecycle_from_naver_status("Running") == "running"

    def test_unknown_status_returns_none(self):
        assert derive_lifecycle_from_naver_status("UNKNOWN") is None

    def test_all_mapped_statuses(self):
        for key in ("BEFORE", "RUNNING", "RESULT", "CANCEL", "CANCELLED", "DELAYED", "SUSPENDED", "POSTPONED"):
            result = derive_lifecycle_from_naver_status(key)
            assert result is not None

    def test_naver_status_map_not_empty(self):
        assert len(NAVER_STATUS_MAP) > 0

    def test_naver_status_map_keys_lowercase(self):
        for key in NAVER_STATUS_MAP:
            assert key == key.lower()
