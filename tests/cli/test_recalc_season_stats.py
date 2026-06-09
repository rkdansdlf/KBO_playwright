from unittest.mock import patch, MagicMock

from src.cli.recalc_season_stats import main


class TestRecalcSeasonStats:
    def test_required_year(self):
        try:
            main()
            assert False, "Should have raised SystemExit"
        except SystemExit:
            pass

    def test_year_with_save(self):
        with patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat:
            mock_bat.return_value = []
            with patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit:
                mock_pit.return_value = []
                result = main()
                assert result is None

    def test_batting_only(self):
        with patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat:
            mock_bat.return_value = []
            with patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit:
                mock_pit.return_value = []
                result = main()
                assert result is None
