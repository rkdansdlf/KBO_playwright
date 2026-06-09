from unittest.mock import MagicMock, patch

from scripts.verification.debug_extract import main


class TestMain:
    @patch("scripts.verification.debug_extract.SessionLocal")
    def test_query_executed(self, mock_session_local):
        mock_session = MagicMock()
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        main()
        mock_session.execute.assert_called_once()
