from unittest.mock import MagicMock, patch

from scripts.supabase.sync_player_basic_first import (
    get_supabase_connection,
    sync_player_basic,
    verify_sync,
)


class TestGetSupabaseConnection:
    @patch("scripts.supabase.sync_player_basic_first.os.getenv")
    def test_missing_env(self, mock_getenv):
        import pytest

        mock_getenv.return_value = None
        with pytest.raises(ValueError, match="SUPABASE_DB_URL"):
            get_supabase_connection()

    @patch("scripts.supabase.sync_player_basic_first.os.getenv")
    def test_success(self, mock_getenv):
        mock_getenv.return_value = "postgresql://test"

        engine = get_supabase_connection()
        assert engine is not None


class TestSyncPlayerBasic:
    @patch("scripts.supabase.sync_player_basic_first.SessionLocal")
    @patch("scripts.supabase.sync_player_basic_first.get_supabase_connection")
    def test_no_players(self, mock_get_supabase, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        mock_supabase = MagicMock()
        mock_get_supabase.return_value = mock_supabase

        sync_player_basic()
        # No error raised
        assert True


class TestVerifySync:
    @patch("scripts.supabase.sync_player_basic_first.SessionLocal")
    @patch("scripts.supabase.sync_player_basic_first.get_supabase_connection")
    def test_counts_match(self, mock_get_supabase, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 5

        mock_supabase = MagicMock()
        mock_supabase.connect.return_value.__enter__.return_value = mock_supabase
        mock_supabase.execute.return_value.scalar.return_value = 5
        mock_get_supabase.return_value = mock_supabase

        verify_sync()
        # No error raised
        assert True
