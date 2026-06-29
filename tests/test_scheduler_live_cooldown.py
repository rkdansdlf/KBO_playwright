"""Tests for scheduler live polling edge cases."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.constants import KST


class TestShouldSkipLiveForPregame:
    """Test the cooldown-based _should_skip_live_for_pregame function."""

    def _call(self, now: datetime, last_run: datetime | None = None, cooldown: str = "30") -> bool:
        from scripts import scheduler

        scheduler.LAST_PREGAME_RUN_TIME = last_run
        with patch.dict(os.environ, {"LIVE_PREGAME_COOLDOWN_SECONDS": cooldown}):
            return scheduler._should_skip_live_for_pregame(now=now)

    def test_no_last_run_returns_false(self) -> None:
        now = datetime(2025, 4, 1, 15, 0, 0, tzinfo=KST)
        assert self._call(now, last_run=None) is False

    def test_within_cooldown_returns_true(self) -> None:
        now = datetime(2025, 4, 1, 15, 0, 0, tzinfo=KST)
        last_run = datetime(2025, 4, 1, 14, 59, 35, tzinfo=KST)
        assert self._call(now, last_run=last_run) is True

    def test_at_cooldown_boundary_returns_false(self) -> None:
        now = datetime(2025, 4, 1, 15, 0, 0, tzinfo=KST)
        last_run = datetime(2025, 4, 1, 14, 59, 30, tzinfo=KST)
        assert self._call(now, last_run=last_run) is False

    def test_past_cooldown_returns_false(self) -> None:
        now = datetime(2025, 4, 1, 15, 0, 0, tzinfo=KST)
        last_run = datetime(2025, 4, 1, 14, 59, 0, tzinfo=KST)
        assert self._call(now, last_run=last_run) is False

    def test_zero_cooldown_returns_false(self) -> None:
        now = datetime(2025, 4, 1, 15, 0, 0, tzinfo=KST)
        last_run = datetime(2025, 4, 1, 14, 59, 0, tzinfo=KST)
        assert self._call(now, last_run=last_run, cooldown="0") is False

    def test_negative_elapsed_returns_false(self) -> None:
        """If last_run is in the future (clock skew), should not skip."""
        now = datetime(2025, 4, 1, 15, 0, 0, tzinfo=KST)
        last_run = datetime(2025, 4, 1, 15, 0, 10, tzinfo=KST)
        assert self._call(now, last_run=last_run) is False

    def test_custom_cooldown(self) -> None:
        now = datetime(2025, 4, 1, 15, 0, 0, tzinfo=KST)
        last_run = datetime(2025, 4, 1, 14, 59, 50, tzinfo=KST)
        assert self._call(now, last_run=last_run, cooldown="15") is True
        assert self._call(now, last_run=last_run, cooldown="5") is False
