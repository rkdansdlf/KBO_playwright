from unittest.mock import MagicMock, patch

from scripts.investigations.check_2026_missing_summaries import check_missing_summaries


class TestCheckMissingSummaries:
    @patch("scripts.investigations.check_2026_missing_summaries.SessionLocal")
    def test_no_missing(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        result = check_missing_summaries()
        # function logs but doesn't return; verify no exception
        assert result is None

    @patch("scripts.investigations.check_2026_missing_summaries.SessionLocal")
    def test_with_missing(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = [
            MagicMock(game_id="G1", game_date="20260401", away_team="LG", home_team="SSG"),
        ]

        check_missing_summaries()
        mock_session.execute.assert_called_once()
