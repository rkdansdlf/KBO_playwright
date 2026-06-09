from argparse import Namespace
from unittest.mock import patch

from src.cli.recalc_season_stats import main


class TestRecalcSeasonStats:
    def test_required_year(self):
        try:
            main()
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_year_with_save(self):
        with patch("argparse.ArgumentParser.parse_args") as mock_parse, \
             patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat, \
             patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit:
            mock_parse.return_value = Namespace(year=2025, series="regular", type="all", save=True)
            mock_bat.return_value = []
            mock_pit.return_value = []
            result = main()
            assert result is None

    def test_batting_only(self):
        with patch("argparse.ArgumentParser.parse_args") as mock_parse, \
             patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat, \
             patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit:
            mock_parse.return_value = Namespace(year=2025, series="regular", type="batting", save=False)
            mock_bat.return_value = []
            mock_pit.return_value = []
            result = main()
            assert result is None
