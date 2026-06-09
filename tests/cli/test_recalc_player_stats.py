from unittest.mock import MagicMock, patch

from src.cli.recalc_player_stats import main


class TestRecalcPlayerStats:
    def test_no_season_errors(self):
        try:
            main([])
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_season_dry_run(self):
        with patch("src.cli.recalc_player_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = [1]
            mock_session.query.return_value.filter.return_value.group_by.return_value.all.return_value = []
            result = main(["--season", "2025", "--dry-run"])
            assert result == 0

    def test_batting_only(self):
        with patch("src.cli.recalc_player_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = [1]
            mock_session.query.return_value.filter.return_value.group_by.return_value.all.return_value = []
            result = main(["--season", "2025", "--batting-only", "--dry-run"])
            assert result == 0

    def test_pitching_only(self):
        with patch("src.cli.recalc_player_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalars.return_value.all.return_value = [1]
            mock_session.query.return_value.filter.return_value.group_by.return_value.all.return_value = []
            result = main(["--season", "2025", "--pitching-only", "--dry-run"])
            assert result == 0
