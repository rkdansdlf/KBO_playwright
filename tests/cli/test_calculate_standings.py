from unittest.mock import MagicMock, patch

from src.cli.calculate_standings import main


class TestCalculateStandings:
    def test_default_year(self):
        with patch("sys.argv", ["calculate_standings"]), patch("src.cli.calculate_standings.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.distinct.return_value.all.return_value = []
            result = main()
            assert result is None

    def test_specific_year(self):
        with patch("sys.argv", ["calculate_standings"]), patch("src.cli.calculate_standings.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            result = main()
            assert result is None

    def test_report_mode(self):
        with patch("sys.argv", ["calculate_standings"]), patch("src.cli.calculate_standings.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            result = main()
            assert result is None
