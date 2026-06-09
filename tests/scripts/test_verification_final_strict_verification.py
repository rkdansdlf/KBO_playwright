from unittest.mock import MagicMock, patch

from scripts.verification.final_strict_verification import final_strict_verification


class TestFinalStrictVerification:
    @patch("scripts.verification.final_strict_verification.sqlite3.connect")
    def test_no_mismatches(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.fetchone.return_value = (10,)

        with patch("scripts.verification.final_strict_verification.pd.read_sql_query") as mock_read:
            mock_read.return_value = MagicMock()
            mock_read.return_value.empty = True

            final_strict_verification()
            assert True

    @patch("scripts.verification.final_strict_verification.sqlite3.connect")
    def test_with_mismatches(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.fetchone.return_value = (10,)

        with patch("scripts.verification.final_strict_verification.pd.read_sql_query") as mock_read:
            mock_read.return_value = MagicMock()
            mock_read.return_value.empty = False

            final_strict_verification()
            assert True
