from unittest.mock import MagicMock, patch

import pytest

from scripts.investigations.inspect_tool import (
    db_inspect_game,
    db_inspect_player,
    run_db_query,
    run_db_summary,
)


class TestRunDbQuery:
    @patch("scripts.investigations.inspect_tool.Path.exists")
    @patch("scripts.investigations.inspect_tool.sqlite3.connect")
    def test_db_not_found(self, mock_connect, mock_exists):
        from pathlib import Path

        mock_exists.return_value = False
        import sys

        with patch.object(sys, "exit", side_effect=SystemExit) as mock_exit, pytest.raises(SystemExit):
            run_db_query(Path("/nonexistent/db.sqlite"), "SELECT 1")
            mock_exit.assert_called_once_with(1)

    @patch("scripts.investigations.inspect_tool.Path.exists")
    @patch("scripts.investigations.inspect_tool.sqlite3.connect")
    @patch("scripts.investigations.inspect_tool.HAS_PANDAS", False)
    def test_db_exists(self, mock_connect, mock_exists):
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [("col",)]
        mock_cursor.fetchall.return_value = [("val",)]

        run_db_query(MagicMock(), "SELECT 1")
        mock_cursor.execute.assert_called_once()


class TestDbInspectGame:
    @patch("scripts.investigations.inspect_tool.run_db_query")
    def test_calls_query(self, mock_run):
        db_inspect_game(MagicMock(), "game123")
        assert mock_run.call_count == 2


class TestDbInspectPlayer:
    @patch("scripts.investigations.inspect_tool.run_db_query")
    def test_calls_query(self, mock_run):
        db_inspect_player(MagicMock(), 42, "2025")
        assert mock_run.call_count == 2


class TestRunDbSummary:
    @patch("scripts.investigations.inspect_tool.Path.exists")
    @patch("scripts.investigations.inspect_tool.sqlite3.connect")
    def test_summary(self, mock_connect, mock_exists):
        mock_exists.return_value = True
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (5,)

        run_db_summary(MagicMock())
        assert mock_cursor.execute.call_count > 0
