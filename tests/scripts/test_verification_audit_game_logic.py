from unittest.mock import MagicMock, patch

from scripts.verification.audit_game_logic import audit_game_logic


class TestAuditGameLogic:
    @patch("scripts.verification.audit_game_logic.SessionLocal")
    def test_no_violations(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session

        mock_session.execute.return_value.mappings.return_value.all.return_value = []

        violations = audit_game_logic(year=2025)
        assert violations == []

    @patch("scripts.verification.audit_game_logic.SessionLocal")
    def test_with_violations(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session

        def side_effect(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.mappings.return_value.all.return_value = [
                MagicMock(
                    game_id="G1",
                    game_date="20250401",
                    home_score=5,
                    away_score=3,
                    home_inning_total=4,
                    away_inning_total=3,
                ),
            ]
            return mock_result

        mock_session.execute = MagicMock(side_effect=side_effect)

        violations = audit_game_logic(year=2025)
        assert len(violations) > 0

    @patch("scripts.verification.audit_game_logic.SessionLocal")
    def test_game_id_filter(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.mappings.return_value.all.return_value = []

        violations = audit_game_logic(game_id="20250401HHSS0")
        assert violations == []
