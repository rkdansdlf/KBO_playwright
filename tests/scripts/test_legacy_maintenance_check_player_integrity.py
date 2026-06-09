from unittest.mock import MagicMock, patch

from scripts.legacy.maintenance.check_player_integrity import run_audit


class TestRunAudit:
    @patch("scripts.legacy.maintenance.check_player_integrity.SessionLocal")
    def test_no_stubs(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        run_audit()
        assert mock_session.execute.call_count > 0
