from unittest.mock import MagicMock, patch

from scripts.supabase.fix_all_supabase_constraints import (
    check_table_exists,
    get_supabase_connection,
    verify_all_constraints,
)


class TestGetSupabaseConnection:
    @patch("scripts.supabase.fix_all_supabase_constraints.os.getenv")
    def test_missing_env(self, mock_getenv):
        import pytest

        mock_getenv.return_value = None
        with pytest.raises(ValueError, match="SUPABASE_DB_URL"):
            get_supabase_connection()


class TestCheckTableExists:
    @patch("scripts.supabase.fix_all_supabase_constraints.get_supabase_connection")
    def test_table_exists(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (True,)

        result = check_table_exists("player_season_batting")
        assert result is True

    @patch("scripts.supabase.fix_all_supabase_constraints.get_supabase_connection")
    def test_table_not_exists(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (False,)

        result = check_table_exists("nonexistent")
        assert result is False


class TestVerifyAllConstraints:
    @patch("scripts.supabase.fix_all_supabase_constraints.check_table_exists")
    @patch("scripts.supabase.fix_all_supabase_constraints.check_table_constraints")
    def test_missing_table(self, mock_check_constraints, mock_check_exists):
        mock_check_exists.return_value = False
        result = verify_all_constraints()
        assert result is False

    @patch("scripts.supabase.fix_all_supabase_constraints.check_table_constraints")
    @patch("scripts.supabase.fix_all_supabase_constraints.check_table_exists")
    def test_all_good(self, mock_check_exists, mock_check_constraints):
        mock_check_exists.return_value = True
        mock_check_constraints.return_value = [
            ("uq_player_season_batting", "UNIQUE", "cols"),
            ("uq_player_season_pitching", "UNIQUE", "cols2"),
        ]
        result = verify_all_constraints()
        assert result is True
