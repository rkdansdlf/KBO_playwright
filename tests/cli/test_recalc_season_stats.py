from argparse import Namespace
from unittest.mock import MagicMock, patch

from src.cli.recalc_season_stats import main


class TestRecalcSeasonStats:
    def test_required_year(self):
        try:
            main()
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_year_with_save(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat,
            patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit,
        ):
            mock_parse.return_value = Namespace(year=2025, series="regular", type="all", save=True)
            mock_bat.return_value = []
            mock_pit.return_value = []
            result = main()
            assert result == 0

    def test_batting_only(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat,
            patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit,
        ):
            mock_parse.return_value = Namespace(year=2025, series="regular", type="batting", save=False)
            mock_bat.return_value = []
            mock_pit.return_value = []
            result = main()
            assert result == 0

    def test_batting_with_data_and_save(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat,
            patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit,
            patch("src.cli.recalc_season_stats.save_batting_stats_safe") as mock_save_bat,
        ):
            mock_parse.return_value = Namespace(year=2025, series="regular", type="batting", save=True)
            mock_bat.return_value = [{"player_id": "p1", "avg": 0.3}]
            mock_pit.return_value = []
            result = main()
            assert result == 0
            mock_save_bat.assert_called_once()

    def test_pitching_with_data_and_save(self):
        pit_stat = MagicMock()
        pit_stat.to_repository_payload.return_value = {"player_id": "p1"}
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat,
            patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit,
            patch("src.cli.recalc_season_stats.save_pitching_stats_to_db") as mock_save_pit,
        ):
            mock_parse.return_value = Namespace(year=2025, series="regular", type="pitching", save=True)
            mock_bat.return_value = []
            mock_pit.return_value = [pit_stat]
            result = main()
            assert result == 0
            mock_save_pit.assert_called_once()

    def test_series_all(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.recalc_season_stats.fallback_batting_from_db") as mock_bat,
            patch("src.cli.recalc_season_stats.fallback_pitching_from_db") as mock_pit,
        ):
            mock_parse.return_value = Namespace(year=2025, series="all", type="all", save=False)
            mock_bat.return_value = []
            mock_pit.return_value = []
            result = main()
            assert result == 0
