from __future__ import annotations

import logging
from unittest.mock import ANY, MagicMock, patch

import pytest

from src.cli.recalc_player_game_stats import (
    _print_batting_records,
    _print_pitching_records,
    main,
    recalc_for_game,
    recalc_for_games_batch,
    run_recalc,
)


class TestRecalcPlayerGameStats:
    def test_no_args_errors(self):
        with pytest.raises(SystemExit):
            main([])

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

    @pytest.mark.parametrize(
        ("arguments", "expected_dry_run"),
        [
            (["--game-id", "G1"], True),
            (["--game-id", "G1", "--save"], False),
        ],
    )
    def test_main_defaults_to_dry_run_unless_save_requested(self, arguments, expected_dry_run):
        with patch("src.cli.recalc_player_game_stats.run_recalc", return_value=0) as recalc:
            assert main(arguments) == 0

        assert recalc.call_args.kwargs["dry_run"] is expected_dry_run


class TestRecalculationHelpers:
    def test_print_record_helpers_log_batting_and_pitching_metrics(self, caplog):
        batting = [
            {"player_id": 1, "player_name": "Kim", "plate_appearances": 4, "hits": 2, "avg": ".500", "ops": "1.000"}
        ]
        pitching = [{"player_id": 2, "player_name": "Lee", "innings_outs": 6, "era": "0.00", "whip": "0.50"}]

        with caplog.at_level(logging.INFO, logger="src.cli.recalc_player_game_stats"):
            _print_batting_records(batting)
            _print_pitching_records(pitching)

        assert "PA=4" in caplog.text
        assert "IP=2.0" in caplog.text

    def test_recalc_for_game_dry_run_prints_aggregates_without_writing(self):
        batting = [{"player_id": 1}]
        pitching = [{"player_id": 2}]

        with (
            patch("src.cli.recalc_player_game_stats.aggregate_game_batting", return_value=batting),
            patch("src.cli.recalc_player_game_stats.aggregate_game_pitching", return_value=pitching),
            patch("src.cli.recalc_player_game_stats._print_batting_records") as print_batting,
            patch("src.cli.recalc_player_game_stats._print_pitching_records") as print_pitching,
            patch("src.cli.recalc_player_game_stats.upsert_player_game_batting") as save_batting,
            patch("src.cli.recalc_player_game_stats.upsert_player_game_pitching") as save_pitching,
        ):
            result = recalc_for_game(MagicMock(), "G1", dry_run=True)

        assert result == {"batting": 1, "pitching": 1}
        print_batting.assert_called_once_with(batting)
        print_pitching.assert_called_once_with(pitching)
        save_batting.assert_not_called()
        save_pitching.assert_not_called()

    def test_recalc_for_game_saves_aggregates(self):
        session = MagicMock()
        batting = [{"player_id": 1}]
        pitching = [{"player_id": 2}]

        with (
            patch("src.cli.recalc_player_game_stats.aggregate_game_batting", return_value=batting),
            patch("src.cli.recalc_player_game_stats.aggregate_game_pitching", return_value=pitching),
            patch("src.cli.recalc_player_game_stats.upsert_player_game_batting", return_value=3) as save_batting,
            patch("src.cli.recalc_player_game_stats.upsert_player_game_pitching", return_value=4) as save_pitching,
        ):
            result = recalc_for_game(session, "G1")

        assert result == {"batting": 3, "pitching": 4}
        save_batting.assert_called_once_with(session, batting)
        save_pitching.assert_called_once_with(session, pitching)

    def test_recalc_for_games_batch_dry_run_does_not_commit(self):
        session = MagicMock()

        with (
            patch("src.cli.recalc_player_game_stats.aggregate_game_batting_batch", return_value=[{"player_id": 1}]),
            patch("src.cli.recalc_player_game_stats.aggregate_game_pitching_batch", return_value=[{"player_id": 2}]),
            patch("src.cli.recalc_player_game_stats.bulk_upsert_player_game_batting") as save_batting,
            patch("src.cli.recalc_player_game_stats.bulk_upsert_player_game_pitching") as save_pitching,
        ):
            result = recalc_for_games_batch(session, ["G1", "G2"], dry_run=True)

        assert result == {"batting": 1, "pitching": 1}
        save_batting.assert_not_called()
        save_pitching.assert_not_called()
        session.commit.assert_not_called()

    def test_recalc_for_games_batch_saves_and_commits_once(self):
        session = MagicMock()
        batting = [{"player_id": 1}]
        pitching = [{"player_id": 2}]

        with (
            patch("src.cli.recalc_player_game_stats.aggregate_game_batting_batch", return_value=batting),
            patch("src.cli.recalc_player_game_stats.aggregate_game_pitching_batch", return_value=pitching),
            patch("src.cli.recalc_player_game_stats.bulk_upsert_player_game_batting", return_value=3) as save_batting,
            patch("src.cli.recalc_player_game_stats.bulk_upsert_player_game_pitching", return_value=4) as save_pitching,
        ):
            result = recalc_for_games_batch(session, ["G1", "G2"])

        assert result == {"batting": 3, "pitching": 4}
        save_batting.assert_called_once_with(session, batting)
        save_pitching.assert_called_once_with(session, pitching)
        session.commit.assert_called_once_with()

    def test_run_recalc_includes_futures_games_in_season_batch(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [10, 11]
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [("G1",), ("G2",)]

        with (
            patch("src.cli.recalc_player_game_stats.SessionLocal") as session_factory,
            patch(
                "src.cli.recalc_player_game_stats.recalc_for_games_batch", return_value={"batting": 3, "pitching": 4}
            ) as recalc_batch,
        ):
            session_factory.return_value.__enter__.return_value = session
            assert run_recalc(season=2025, include_futures=True) == 0

        session.execute.assert_called_once_with(
            ANY,
            {"year": 2025, "codes": [0, 5]},
        )
        recalc_batch.assert_called_once_with(session, ["G1", "G2"], dry_run=False)

    def test_run_recalc_warns_when_no_games_match(self, caplog):
        session = MagicMock()

        with (
            patch("src.cli.recalc_player_game_stats.SessionLocal") as session_factory,
            patch("src.cli.recalc_player_game_stats.recalc_for_game") as recalc,
            caplog.at_level(logging.WARNING, logger="src.cli.recalc_player_game_stats"),
        ):
            session_factory.return_value.__enter__.return_value = session
            assert run_recalc() == 0

        assert "No completed games matched" in caplog.text
        recalc.assert_not_called()
