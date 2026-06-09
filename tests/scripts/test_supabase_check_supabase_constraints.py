from unittest.mock import MagicMock, patch

from scripts.supabase.check_supabase_constraints import check_supabase_structure


class TestCheckSupabaseStructure:
    @patch("scripts.supabase.check_supabase_constraints.os.getenv")
    def test_missing_env(self, mock_getenv):
        mock_getenv.return_value = None
        result = check_supabase_structure()
        assert result is False

    @patch("scripts.supabase.check_supabase_constraints.os.getenv")
    @patch("scripts.supabase.check_supabase_constraints.create_engine")
    def test_success(self, mock_create_engine, mock_getenv):
        mock_getenv.return_value = "postgresql://test"
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_conn.execute.return_value.fetchone.return_value = None

        result = check_supabase_structure()
        assert result is True

    @patch("scripts.supabase.check_supabase_constraints.os.getenv")
    @patch("scripts.supabase.check_supabase_constraints.create_engine")
    def test_connection_failure(self, mock_create_engine, mock_getenv):
        mock_getenv.return_value = "postgresql://test"
        mock_create_engine.side_effect = Exception("connection failed")
        result = check_supabase_structure()
        assert result is False
