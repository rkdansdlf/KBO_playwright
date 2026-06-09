from unittest.mock import patch, MagicMock

from src.cli.repair_game_stats import main


class TestRepairGameStats:
    def test_default_all(self):
        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.count.return_value = 0
            result = main([])
            assert result is None

    def test_batting_only(self):
        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.count.return_value = 0
            result = main(["--type", "batting"])
            assert result is None

    def test_pitching_only(self):
        with patch("src.cli.repair_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.count.return_value = 0
            result = main(["--type", "pitching"])
            assert result is None
