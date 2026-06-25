from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from src.cli.backfill_advanced_stats import (
    _assign_team,
    _backfill_baserunning,
    _backfill_batting,
    _backfill_fielding,
    _backfill_pitching,
    _build_player_team_map,
    backfill_stats,
    main,
)


class TestBuildPlayerTeamMap:
    def test_builds_map_from_batting(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.player_id = 1
        mock_row.team_code = "LG"
        mock_row.id = 1

        mock_session.query.return_value.group_by.return_value.all.return_value = [
            (1, "LG", 10),
        ]

        result = _build_player_team_map(mock_session)
        assert result[1] == ("LG", 10)

    def test_prefers_higher_count(self):
        mock_session = MagicMock()
        mock_session.query.return_value.group_by.return_value.all.return_value = [
            (1, "LG", 5),
            (1, "OB", 10),
        ]

        result = _build_player_team_map(mock_session)
        assert result[1] == ("OB", 10)

    def test_returns_empty_when_no_data(self):
        mock_session = MagicMock()
        mock_session.query.return_value.group_by.return_value.all.return_value = []

        result = _build_player_team_map(mock_session)
        assert result == {}


class TestAssignTeam:
    def test_assigns_team_code(self):
        stats = [{"player_id": 1, "hits": 10}, {"player_id": 2, "hits": 5}]
        team_map = {1: ("LG", 10), 2: ("OB", 5)}

        result = _assign_team(stats, team_map, target_key="team_code")
        assert len(result) == 2
        assert result[0]["team_code"] == "LG"
        assert result[1]["team_code"] == "OB"

    def test_filters_out_unmapped(self):
        stats = [{"player_id": 1, "hits": 10}, {"player_id": 99, "hits": 5}]
        team_map = {1: ("LG", 10)}

        result = _assign_team(stats, team_map, target_key="team_code")
        assert len(result) == 1
        assert result[0]["player_id"] == 1

    def test_assigns_team_id(self):
        stats = [{"player_id": 1, "hits": 10}]
        team_map = {1: ("LG", 10)}

        result = _assign_team(stats, team_map, target_key="team_id")
        assert result[0]["team_id"] == "LG"


class TestBackfillBatting:
    def test_no_stats_returns_early(self):
        mock_session = MagicMock()
        with patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg:
            mock_agg.aggregate_batting_season_bulk.return_value = []
            _backfill_batting(mock_session, 2025, "regular", {})

    def test_saves_valid_stats(self):
        mock_session = MagicMock()
        stats = [{"player_id": 1, "hits": 10}]
        team_map = {1: ("LG", 5)}

        with (
            patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg,
            patch("src.cli.backfill_advanced_stats.save_batting_stats_safe") as mock_save,
        ):
            mock_agg.aggregate_batting_season_bulk.return_value = stats
            _backfill_batting(mock_session, 2025, "regular", team_map)
            mock_save.assert_called_once()


class TestBackfillPitching:
    def test_no_stats_returns_early(self):
        mock_session = MagicMock()
        with patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg:
            mock_agg.aggregate_pitching_season_bulk.return_value = []
            _backfill_pitching(mock_session, 2025, "regular", {})

    def test_saves_valid_stats(self):
        mock_session = MagicMock()
        stats = [{"player_id": 1, "era": 3.0}]
        team_map = {1: ("LG", 5)}

        with (
            patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg,
            patch("src.cli.backfill_advanced_stats.save_pitching_stats_to_db") as mock_save,
        ):
            mock_agg.aggregate_pitching_season_bulk.return_value = stats
            _backfill_pitching(mock_session, 2025, "regular", team_map)
            mock_save.assert_called_once()


class TestBackfillBaserunning:
    def test_no_stats_returns_early(self):
        mock_session = MagicMock()
        mock_repo = MagicMock()
        with patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg:
            mock_agg.aggregate_baserunning_season_bulk.return_value = []
            _backfill_baserunning(mock_session, 2025, "regular", {}, mock_repo)

    def test_upserts_valid_stats(self):
        mock_session = MagicMock()
        mock_repo = MagicMock()
        stats = [{"player_id": 1, "sb": 10}]
        team_map = {1: ("LG", 5)}

        with patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg:
            mock_agg.aggregate_baserunning_season_bulk.return_value = stats
            _backfill_baserunning(mock_session, 2025, "regular", team_map, mock_repo)
            mock_repo.upsert_many.assert_called_once()


class TestBackfillFielding:
    def test_no_stats_returns_early(self):
        mock_session = MagicMock()
        mock_repo = MagicMock()
        with patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg:
            mock_agg.aggregate_fielding_season_bulk.return_value = []
            _backfill_fielding(mock_session, 2025, "regular", {}, mock_repo)

    def test_upserts_valid_stats(self):
        mock_session = MagicMock()
        mock_repo = MagicMock()
        stats = [{"player_id": 1, "errors": 2}]
        team_map = {1: ("LG", 5)}

        with patch("src.cli.backfill_advanced_stats.SeasonStatAggregator") as mock_agg:
            mock_agg.aggregate_fielding_season_bulk.return_value = stats
            _backfill_fielding(mock_session, 2025, "regular", team_map, mock_repo)
            mock_repo.upsert_many.assert_called_once()


class TestBackfillStats:
    def test_calls_all_backfill_functions(self):
        with (
            patch("src.cli.backfill_advanced_stats.SessionLocal") as mock_sf,
            patch("src.cli.backfill_advanced_stats.PlayerSeasonFieldingRepository"),
            patch("src.cli.backfill_advanced_stats.PlayerSeasonBaserunningRepository"),
            patch("src.cli.backfill_advanced_stats._build_player_team_map") as mock_build,
            patch("src.cli.backfill_advanced_stats._backfill_batting") as mock_bat,
            patch("src.cli.backfill_advanced_stats._backfill_pitching") as mock_pit,
            patch("src.cli.backfill_advanced_stats._backfill_baserunning") as mock_br,
            patch("src.cli.backfill_advanced_stats._backfill_fielding") as mock_fld,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_build.return_value = {1: ("LG", 5)}

            backfill_stats([2024, 2025], "regular")

            assert mock_bat.call_count == 2
            assert mock_pit.call_count == 2
            assert mock_br.call_count == 2
            assert mock_fld.call_count == 2


class TestBackfillAdvancedStatsCLI:
    def test_default_years(self):
        with patch("src.cli.backfill_advanced_stats.backfill_stats") as mock:
            mock.return_value = None
            result = main([])
            assert result == 0
            mock.assert_called_once()
            args, _ = mock.call_args
            assert 2020 in args[0]

    def test_specific_year(self):
        with patch("src.cli.backfill_advanced_stats.backfill_stats") as mock:
            result = main(["--years", "2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2025]

    def test_with_series(self):
        with patch("src.cli.backfill_advanced_stats.backfill_stats") as mock:
            result = main(["--years", "2024", "--series", "postseason"])
            assert result == 0
            args, _ = mock.call_args
            assert args[1] == "postseason"

    def test_year_range(self):
        with patch("src.cli.backfill_advanced_stats.backfill_stats") as mock:
            result = main(["--years", "2023-2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2023, 2024, 2025]
