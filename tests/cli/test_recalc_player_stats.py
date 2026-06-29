from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.recalc_player_stats import (
    _get_player_teams,
    _get_regular_season_ids,
    main,
    run_recalc,
)


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


class TestGetRegularSeasonIds:
    def test_returns_season_ids(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.scalars.return_value.all.return_value = [1, 2]
        result = _get_regular_season_ids(mock_session, 2025)
        assert result == [1, 2]

    def test_returns_empty_when_no_match(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.scalars.return_value.all.return_value = []
        result = _get_regular_season_ids(mock_session, 2030)
        assert result == []


class TestGetPlayerTeams:
    def test_returns_team_map(self):
        from src.models.game import GameBattingStat

        mock_session = MagicMock()
        row1 = MagicMock()
        row1.player_id = 1
        row1.canonical_team_code = "LG"
        row1.cnt = 10
        mock_session.query.return_value.join.return_value.filter.return_value.group_by.return_value.order_by.return_value.all.return_value = [
            row1,
        ]
        result = _get_player_teams(mock_session, [1], GameBattingStat)
        assert result[1] == "LG"

    def test_multiple_teams(self):
        from src.models.game import GameBattingStat

        mock_session = MagicMock()
        row1 = MagicMock()
        row1.player_id = 1
        row1.canonical_team_code = "LG"
        row1.cnt = 5
        mock_session.query.return_value.join.return_value.filter.return_value.group_by.return_value.order_by.return_value.all.return_value = [
            row1,
        ]
        result = _get_player_teams(mock_session, [1], GameBattingStat)
        assert result[1] == "LG"


class TestRunRecalc:
    def test_returns_zero_on_success(self):
        with (
            patch("src.cli.recalc_player_stats.SessionLocal") as mock_sf,
            patch("src.cli.recalc_player_stats._get_regular_season_ids") as mock_ids,
            patch("src.cli.recalc_player_stats._get_player_teams") as mock_teams,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_ids.return_value = [1]
            mock_teams.return_value = {1: "LG"}
            mock_session.query.return_value.filter.return_value.all.return_value = []

            result = run_recalc(2025, dry_run=True)
            assert result == 0


class TestRecalcPlayerStatsEdgeCases:
    def test_main_non_dry_run(self):
        with patch("src.cli.recalc_player_stats.run_recalc") as mock:
            mock.return_value = 0
            result = main(["--season", "2025"])
            assert result == 0

    def test_main_both_flags(self):
        with patch("src.cli.recalc_player_stats.run_recalc") as mock:
            mock.return_value = 0
            result = main(["--season", "2025", "--batting-only", "--pitching-only"])
            assert result == 0
