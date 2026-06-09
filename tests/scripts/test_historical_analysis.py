from unittest.mock import MagicMock, patch


class TestHistoricalAnalysis:
    def test_analyze_historical_leaders(self):
        with patch("scripts.historical_analysis.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.join.return_value.filter.return_value.group_by.return_value.order_by.return_value.limit.return_value.all.return_value = []
            from scripts.historical_analysis import analyze_historical_leaders
            analyze_historical_leaders()
