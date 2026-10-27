from __future__ import annotations

import pytest

from src.utils.game_state import (
    ALLOWED_TRANSITIONS,
    derive_lifecycle_from_naver_status,
    is_terminal,
    validate_transition,
)


class TestIsTerminal:
    def test_cancelled(self):
        assert is_terminal("cancelled") is True

    def test_final(self):
        assert is_terminal("final") is True

    def test_running(self):
        assert is_terminal("running") is False

    def test_before(self):
        assert is_terminal("before") is False


class TestACancelledGameCanLeaveTheResultWait:
    """A game cancelled while its result was pending has to be able to say so.

    ``result_pending_stabilization`` is in ``RELAY_ACTIVE_STATES``, so a game
    sitting there is polled again on every live cycle. The table had no edge
    from it to ``cancelled``, so a rain-out after the result window left the
    game retrying indefinitely: 20261026HTSS0 was recorded CANCELLED at 18:01
    and was still being fetched at 20:00, adding a failed run each time.
    """

    def test_the_result_wait_can_be_cancelled(self) -> None:
        valid, err = validate_transition("result_pending_stabilization", "cancelled")

        assert valid is True
        assert err is None

    def test_the_result_wait_can_still_finish(self) -> None:
        valid, _ = validate_transition("result_pending_stabilization", "final")

        assert valid is True

    def test_a_cancelled_game_stops_being_a_relay_target(self) -> None:
        # The transition only helps if the state it lands in is not itself
        # polled; ``cancelled`` is terminal.
        assert is_terminal("cancelled") is True


class TestValidateTransition:
    def test_valid_before_to_running(self):
        valid, err = validate_transition(None, "running")
        assert valid is True
        assert err is None

    def test_valid_from_none(self):
        valid, err = validate_transition(None, "final")
        assert valid is True

    def test_invalid_terminal_to_running(self):
        valid, err = validate_transition("cancelled", "running")
        assert valid is False
        assert err is not None

    def test_unknown_state(self):
        valid, err = validate_transition(None, "unknown_state")
        assert valid is False
        assert "unknown" in err

    def test_none_current_always_valid(self):
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
    def test_from_before(self):
        targets = {t[1] for t in ALLOWED_TRANSITIONS if t[0] == "before"}
        assert "running" in targets
        assert "cancelled" in targets

    def test_from_running(self):
        targets = {t[1] for t in ALLOWED_TRANSITIONS if t[0] == "running"}
        assert "delayed" in targets
        assert "suspended" in targets
        assert "cancelled" in targets
