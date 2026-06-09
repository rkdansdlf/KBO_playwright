from unittest.mock import MagicMock, patch

from scripts.supabase.check_supabase_data import check_supabase_data


class TestCheckSupabaseData:
    @patch("scripts.supabase.check_supabase_data.os.getenv")
    def test_missing_env(self, mock_getenv):
        mock_getenv.return_value = None
        result = check_supabase_data()
        assert result is False

    @patch("scripts.supabase.check_supabase_data.os.getenv")
    @patch("scripts.supabase.check_supabase_data.create_engine")
    def test_success(self, mock_create_engine, mock_getenv):
        mock_getenv.return_value = "postgresql://test"
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_conn.execute.return_value.scalar.return_value = 0
        mock_conn.execute.return_value.fetchall.return_value = []

        result = check_supabase_data()
        assert result is True

    @patch("scripts.supabase.check_supabase_data.os.getenv")
    @patch("scripts.supabase.check_supabase_data.create_engine")
    def test_error(self, mock_create_engine, mock_getenv):
        mock_getenv.return_value = "postgresql://test"
        mock_create_engine.side_effect = Exception("error")
        result = check_supabase_data()
        assert result is False
