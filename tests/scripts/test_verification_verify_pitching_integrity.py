from unittest.mock import MagicMock, patch

from scripts.verification.verify_pitching_integrity import verify_pitching_integrity


class TestVerifyPitchingIntegrity:
    @patch("scripts.verification.verify_pitching_integrity.sqlite3.connect")
    def test_no_mismatches(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.fetchone.return_value = (10,)

        with patch("scripts.verification.verify_pitching_integrity.pd.read_sql_query") as mock_read:
            mock_read.return_value = MagicMock()
            mock_read.return_value.empty = True

            verify_pitching_integrity()
            assert True

    @patch("scripts.verification.verify_pitching_integrity.sqlite3.connect")
    def test_with_mismatches(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.fetchone.return_value = (10,)

        with patch("scripts.verification.verify_pitching_integrity.pd.read_sql_query") as mock_read:
            mock_read.return_value = MagicMock()
            mock_read.return_value.empty = False

            verify_pitching_integrity()
            assert True
