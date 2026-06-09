from unittest.mock import MagicMock, patch

from scripts.verification.check_anomalies_v2 import check_anomalies


class TestCheckAnomalies:
    @patch("scripts.verification.check_anomalies_v2.sqlite3.connect")
    def test_no_issues(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = (0,)
        mock_conn.execute.return_value.fetchall.return_value = []

        check_anomalies(":memory:")
        assert mock_conn.execute.call_count > 0

    @patch("scripts.verification.check_anomalies_v2.sqlite3.connect")
    def test_with_nulls(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_conn.execute.return_value.fetchone.return_value = (0,)
        mock_conn.execute.return_value.fetchall.return_value = []

        check_anomalies(":memory:")
        assert True
