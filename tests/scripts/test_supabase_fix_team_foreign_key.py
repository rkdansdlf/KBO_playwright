from unittest.mock import MagicMock, patch

from scripts.supabase.fix_team_foreign_key import (
    analyze_team_history,
    get_supabase_connection,
    implement_option1,
)


class TestGetSupabaseConnection:
    @patch("scripts.supabase.fix_team_foreign_key.os.getenv")
    def test_missing_env(self, mock_getenv):
        import pytest

        mock_getenv.return_value = None
        with pytest.raises(ValueError, match="SUPABASE_DB_URL"):
            get_supabase_connection()


class TestAnalyzeTeamHistory:
    @patch("scripts.supabase.fix_team_foreign_key.get_supabase_connection")
    def test_no_duplicates(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = [
            [("col1", "text", "YES")],
            [],
            [("LG", 1)],
        ]

        all_codes, duplicates = analyze_team_history()
        assert duplicates == []


class TestImplementOption1:
    @patch("scripts.supabase.fix_team_foreign_key.get_supabase_connection")
    def test_drops_constraints(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        implement_option1()
        assert mock_cursor.execute.call_count == 2
