from __future__ import annotations

from src.utils.game_status import (
    ALL_GAME_STATUSES,
    LIVE_GAME_STATUSES,
    TERMINAL_GAME_STATUSES,
    is_terminal_status,
)


class TestGameStatus:
    def test_terminal_states_defined(self):
        assert isinstance(TERMINAL_GAME_STATUSES, set)
        assert len(TERMINAL_GAME_STATUSES) > 0

    def test_lifecycle_states_defined(self):
        assert isinstance(LIVE_GAME_STATUSES, set)
        assert len(LIVE_GAME_STATUSES) > 0

    def test_all_states_defined(self):
        assert isinstance(ALL_GAME_STATUSES, set)
        assert len(ALL_GAME_STATUSES) > 0

    def test_is_terminal_true(self):
        assert is_terminal_status("COMPLETED") is True
        assert is_terminal_status("CANCELLED") is True

    def test_is_terminal_false(self):
        assert is_terminal_status("SCHEDULED") is False
        assert is_terminal_status("LIVE") is False
