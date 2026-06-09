from src.utils.game_state import (
    LIFECYCLE_STATES,
    NAVER_STATUS_MAP,
    RELAY_ACTIVE_STATES,
    TERMINAL_STATES,
    derive_lifecycle_from_naver_status,
    validate_transition,
)


class TestValidateTransition:
    def test_none_current_is_valid(self):
        assert validate_transition(None, "running") == (True, None)

    def test_unknown_lifecycle_state(self):
        assert validate_transition("running", "invalid_state") == (False, "unknown_lifecycle_state_invalid_state")

    def test_unknown_current_state(self):
        assert validate_transition("unknown", "running") == (False, "unknown_current_state_unknown")

    def test_terminal_cancelled_cannot_transition(self):
        assert validate_transition("cancelled", "running")[0] is False

    def test_terminal_final_cannot_transition(self):
        assert validate_transition("final", "running")[0] is False

    def test_valid_before_to_running(self):
        assert validate_transition("before", "running") == (True, None)

    def test_invalid_before_to_final(self):
        assert validate_transition("before", "final")[0] is False

    def test_result_to_final_valid(self):
        assert validate_transition("result_pending_stabilization", "final") == (True, None)


class TestDeriveLifecycleFromNaverStatus:
    def test_none_returns_none(self):
        assert derive_lifecycle_from_naver_status(None) is None

    def test_before_mapping(self):
        assert derive_lifecycle_from_naver_status("BEFORE") == "before"

    def test_running_mapping(self):
        assert derive_lifecycle_from_naver_status("RUNNING") == "running"

    def test_result_mapping(self):
        assert derive_lifecycle_from_naver_status("RESULT") == "result_pending_stabilization"

    def test_cancel_mapping(self):
        assert derive_lifecycle_from_naver_status("CANCEL") == "cancelled"

    def test_cancelled_mapping(self):
        assert derive_lifecycle_from_naver_status("CANCELLED") == "cancelled"

    def test_delayed_mapping(self):
        assert derive_lifecycle_from_naver_status("DELAYED") == "delayed"

    def test_suspended_mapping(self):
        assert derive_lifecycle_from_naver_status("SUSPENDED") == "suspended"

    def test_case_insensitive(self):
        assert derive_lifecycle_from_naver_status("running") == "running"
        assert derive_lifecycle_from_naver_status("Before") == "before"

    def test_unknown_status_returns_none(self):
        assert derive_lifecycle_from_naver_status("UNKNOWN") is None


class TestConstants:
    def test_lifecycle_states_contains_all(self):
        assert "before" in LIFECYCLE_STATES
        assert "final" in LIFECYCLE_STATES
        assert "running" in LIFECYCLE_STATES

    def test_terminal_states(self):
        assert {"cancelled", "final"} == TERMINAL_STATES

    def test_relay_active_states(self):
        assert "running" in RELAY_ACTIVE_STATES
        assert "final" not in RELAY_ACTIVE_STATES

    def test_naver_status_map_symmetric(self):
        for key in ("before", "running", "result", "cancel", "delayed", "suspended"):
            assert key in NAVER_STATUS_MAP
            assert NAVER_STATUS_MAP[key] == key.upper()
