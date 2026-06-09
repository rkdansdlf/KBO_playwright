from unittest.mock import MagicMock, patch

from src.cli.recalc_player_game_stats import main


class TestRecalcPlayerGameStats:
    def test_no_args_errors(self):
        try:
            main([])
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_game_id_dry_run(self):
        with patch("src.cli.recalc_player_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--game-id", "20250401LGSS0", "--dry-run"])
            assert result == 0

    def test_season(self):
        with patch("src.cli.recalc_player_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = []
            result = main(["--season", "2025", "--dry-run"])
            assert result == 0

    def test_date(self):
        with patch("src.cli.recalc_player_game_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--date", "20250401", "--dry-run"])
            assert result == 0
