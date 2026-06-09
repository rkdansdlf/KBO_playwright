from unittest.mock import MagicMock, patch


class TestHistoricalGapAnalysis:
    def test_analyze_gaps(self):
        with patch("scripts.historical_gap_analysis.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_batting = MagicMock()
            mock_batting.season = 1990
            mock_batting.cnt = 100
            mock_pitching = MagicMock()
            mock_pitching.season = 1990
            mock_pitching.cnt = 80
            mock_game = MagicMock()
            mock_game.yr = 1990
            mock_game.cnt = 50
            mock_session.execute.return_value.all.side_effect = [
                [],
                [mock_batting],
                [mock_pitching],
                [mock_game],
            ]
            from scripts.historical_gap_analysis import analyze_gaps
            analyze_gaps()
