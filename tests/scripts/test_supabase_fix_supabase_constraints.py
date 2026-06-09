from unittest.mock import MagicMock, patch

from scripts.supabase.fix_supabase_constraints import (
    check_existing_constraints,
    check_table_structure,
    get_supabase_connection,
)


class TestGetSupabaseConnection:
    @patch("scripts.supabase.fix_supabase_constraints.os.getenv")
    def test_missing_env(self, mock_getenv):
        import pytest

        mock_getenv.return_value = None
        with pytest.raises(ValueError, match="SUPABASE_DB_URL"):
            get_supabase_connection()


class TestCheckTableStructure:
    @patch("scripts.supabase.fix_supabase_constraints.get_supabase_connection")
    def test_table_exists(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (True,)

        result = check_table_structure()
        assert result is True

    @patch("scripts.supabase.fix_supabase_constraints.get_supabase_connection")
    def test_table_not_exists(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (False,)

        result = check_table_structure()
        assert result is False


class TestCheckExistingConstraints:
    @patch("scripts.supabase.fix_supabase_constraints.get_supabase_connection")
    def test_empty(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []

        result = check_existing_constraints()
        assert result == []
