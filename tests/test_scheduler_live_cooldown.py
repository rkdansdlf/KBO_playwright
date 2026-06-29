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


class TestGetLivePollIntervalSeconds:
    """Test the _get_live_poll_interval_seconds function with mocked DB."""

    def _call_with_rows(self, rows: list[tuple]) -> int:
        from scripts import scheduler

        mock_session = MagicMock()
        mock_session.execute.return_value.all.return_value = rows
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("scripts.scheduler.SessionLocal", return_value=mock_session):
            return scheduler._get_live_poll_interval_seconds()

    def test_no_games_returns_1800(self) -> None:
        assert self._call_with_rows([]) == 1800

    def test_live_game_returns_10(self) -> None:
        rows = [("LIVE", "running", "14:00", None)]
        assert self._call_with_rows(rows) == 10

    def test_delayed_game_returns_60(self) -> None:
        rows = [("DELAYED", None, "14:00", None)]
        assert self._call_with_rows(rows) == 60

    def test_suspended_game_returns_60(self) -> None:
        rows = [("SUSPENDED", "suspended", "14:00", None)]
        assert self._call_with_rows(rows) == 60

    def test_all_terminal_recent_update_returns_60(self) -> None:
        recent = (datetime.now(KST) - timedelta(minutes=5)).isoformat()
        rows = [("COMPLETED", "final", "14:00", recent)]
        assert self._call_with_rows(rows) == 60

    def test_all_terminal_old_update_returns_1800(self) -> None:
        old = (datetime.now(KST) - timedelta(minutes=30)).isoformat()
        rows = [("COMPLETED", "final", "14:00", old)]
        assert self._call_with_rows(rows) == 1800

    def test_scheduled_soon_returns_30(self) -> None:
        now = datetime.now(KST)
        future_start = (now + timedelta(minutes=10)).strftime("%H:%M")
        rows = [("SCHEDULED", None, future_start, None)]
        assert self._call_with_rows(rows) == 30

    def test_scheduled_far_returns_120(self) -> None:
        now = datetime.now(KST)
        future_start = (now + timedelta(hours=2)).strftime("%H:%M")
        rows = [("SCHEDULED", None, future_start, None)]
        assert self._call_with_rows(rows) == 120

    def test_mixed_live_and_terminal_returns_10(self) -> None:
        rows = [
            ("COMPLETED", "final", "13:00", None),
            ("LIVE", "running", "14:00", None),
        ]
        assert self._call_with_rows(rows) == 10

    def test_db_error_returns_120(self) -> None:
        from sqlalchemy.exc import SQLAlchemyError

        mock_session = MagicMock()
        mock_session.execute.side_effect = SQLAlchemyError("connection lost")
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("scripts.scheduler.SessionLocal", return_value=mock_session):
            from scripts import scheduler

            assert scheduler._get_live_poll_interval_seconds() == 120
