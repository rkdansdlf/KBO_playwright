"""Unit tests for scheduler pregame DB error handling and SQLite integrity guard."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

from scripts.scheduler import _pregame_refresh_summary, _process_pregame_date
from src.db.sqlite_integrity import sqlite_path_from_url


def test_sqlite_path_from_url_ignores_dot_underscore() -> None:
    """Ensure macOS ._* AppleDouble resource fork paths are ignored."""
    assert sqlite_path_from_url("sqlite:///data/._kbo_dev.db") is None
    assert sqlite_path_from_url("sqlite:///data/kbo_dev.db") == Path("data/kbo_dev.db")


def test_pregame_refresh_summary_handles_database_error() -> None:
    """Ensure _pregame_refresh_summary returns fallback tuple on file is not a database error."""
    mock_session = MagicMock()
    mock_session.execute.side_effect = SQLAlchemyError("(sqlite3.DatabaseError) file is not a database")

    with patch("scripts.scheduler.SessionLocal", return_value=mock_session):
        res = _pregame_refresh_summary("20260811")
        assert res == (0, 0, 0)


def test_pregame_refresh_summary_handles_general_scheduler_exception() -> None:
    """Ensure _pregame_refresh_summary handles general DB errors gracefully."""
    mock_session = MagicMock()
    mock_session.execute.side_effect = SQLAlchemyError("OperationalError: database locked")

    with patch("scripts.scheduler.SessionLocal", return_value=mock_session):
        res = _pregame_refresh_summary("20260811")
        assert res == (0, 0, 0)


def test_process_pregame_date_graceful_on_db_corruption() -> None:
    """Ensure _process_pregame_date returns 0 without crashing when DB returns 0 scheduled games."""
    with patch("scripts.scheduler._pregame_refresh_summary", return_value=(0, 0, 0)):
        result = _process_pregame_date("20260811", refresh_only_missing=True, alert_on_missing=False)
        assert result == 0
