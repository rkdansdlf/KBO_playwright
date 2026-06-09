from unittest.mock import MagicMock, patch

from scripts.verification.remediate_kbo_data import get_invalid_games_for_year


class TestGetInvalidGamesForYear:
    @patch("scripts.verification.remediate_kbo_data.audit_game_logic")
    @patch("scripts.verification.remediate_kbo_data.SessionLocal")
    def test_no_invalid_games(self, mock_session_local, mock_audit):
        mock_audit.return_value = []
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.mappings.return_value.all.return_value = []

        result = get_invalid_games_for_year(2025)
        assert result == []

    @patch("scripts.verification.remediate_kbo_data.audit_game_logic")
    @patch("scripts.verification.remediate_kbo_data.SessionLocal")
    def test_with_violations(self, mock_session_local, mock_audit):
        mock_audit.return_value = [{"game_id": "G1", "game_date": "20250401", "reason": "test"}]
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session

        mock_row = {"game_id": "G1", "game_date": "2025-04-01"}

        def mock_execute_side_effect(*args, **kwargs):
            result = MagicMock()
            result.mappings.return_value.all.return_value = [mock_row]
            return result

        mock_session.execute.side_effect = mock_execute_side_effect

        result = get_invalid_games_for_year(2025)
        assert len(result) == 1
        assert result[0]["game_id"] == "G1"
