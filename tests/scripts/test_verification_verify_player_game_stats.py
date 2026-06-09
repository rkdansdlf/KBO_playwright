from unittest.mock import MagicMock, patch

from scripts.verification.verify_player_game_stats import (
    check_coverage,
    check_draw_missing_sources,
    check_duplicates,
    check_nulls,
    check_rate_stats,
    main,
)


class TestCheckDuplicates:
    @patch("scripts.verification.verify_player_game_stats.SessionLocal")
    def test_no_duplicates(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 0

        result = check_duplicates(mock_session)
        assert result == []

    @patch("scripts.verification.verify_player_game_stats.SessionLocal")
    def test_with_duplicates(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 3

        result = check_duplicates(mock_session)
        assert len(result) > 0


class TestCheckNulls:
    @patch("scripts.verification.verify_player_game_stats.SessionLocal")
    def test_no_nulls(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 0

        result = check_nulls(mock_session)
        assert result == []


class TestCheckRateStats:
    @patch("scripts.verification.verify_player_game_stats.SessionLocal")
    def test_ok(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 0

        result = check_rate_stats(mock_session)
        assert isinstance(result, list)


class TestCheckCoverage:
    def test_empty(self):
        result = check_coverage(MagicMock())
        assert isinstance(result, list)

    def test_with_data(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            (2025, "COMPLETED", 100, 95),
        ]
        result = check_coverage(mock_session, verbose=True)
        assert isinstance(result, list)


class TestCheckDrawMissingSources:
    def test_no_issues(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar.return_value = 0
        result = check_draw_missing_sources(mock_session)
        assert result == []


class TestMain:
    @patch("scripts.verification.verify_player_game_stats.SessionLocal")
    def test_returns_zero(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.execute.return_value.scalar.return_value = 0
        mock_session.execute.return_value.fetchall.return_value = []

        result = main([])
        assert result == 0
