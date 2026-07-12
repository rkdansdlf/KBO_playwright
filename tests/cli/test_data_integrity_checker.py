"""Tests for data integrity checker."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.cli import data_integrity_checker as checker_module
from src.cli.data_integrity_checker import (
    CheckResult,
    IntegrityReport,
    check_all_terminal_status,
    check_child_stats_exist,
    check_duplicate_games,
    check_game_status_populated,
    check_games_exist,
    check_no_null_player_ids,
    check_scores_populated,
    check_futures_daily_integrity,
    main,
    run_integrity_checks,
)


def _make_session(
    *,
    game_rows: list[dict[str, Any]] | None = None,
    batting_rows: list[dict[str, Any]] | None = None,
    pitching_rows: list[dict[str, Any]] | None = None,
    lineup_rows: list[dict[str, Any]] | None = None,
    inning_rows: list[dict[str, Any]] | None = None,
) -> MagicMock:
    """Create a mock session with configurable query results."""
    session = MagicMock()

    game_rows = game_rows or []
    batting_rows = batting_rows or []
    pitching_rows = pitching_rows or []
    lineup_rows = lineup_rows or []
    inning_rows = inning_rows or []

    query = session.query.return_value
    filter_chain = query.filter.return_value

    all_rows = game_rows + batting_rows + pitching_rows + lineup_rows + inning_rows

    filter_chain.all.return_value = all_rows
    filter_chain.count.return_value = len(all_rows)
    filter_chain.first.return_value = None

    session.query.return_value.filter.return_value.scalar.side_effect = lambda: 0

    return session


class TestCheckGamesExist:
    def test_no_games_fails(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 0

        result = check_games_exist(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "No game rows found" in result.message

    def test_games_exist_passes(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 5

        result = check_games_exist(session, _date(2026, 6, 24))
        assert result.passed is True
        assert "Found 5 game(s)" in result.message


class TestCheckGameStatusPopulated:
    def test_all_populated_passes(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.count.side_effect = [5, 0]

        result = check_game_status_populated(session, _date(2026, 6, 24))
        assert result.passed is True

    def test_null_status_fails(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.count.side_effect = [5, 2]

        result = check_game_status_populated(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "2 of 5 games have NULL game_status" in result.message


class TestCheckAllTerminalStatus:
    def test_all_terminal_passes(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_status = "COMPLETED"
        game_mock.game_id = "20260624LGSS0"
        game_mock.home_team = "LG"
        game_mock.away_team = "SSG"
        session.query.return_value.filter.return_value.all.return_value = [game_mock]

        result = check_all_terminal_status(session, _date(2026, 6, 24))
        assert result.passed is True

    def test_non_terminal_fails(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_status = "LIVE"
        game_mock.game_id = "20260624LGSS0"
        game_mock.home_team = "LG"
        game_mock.away_team = "SSG"
        session.query.return_value.filter.return_value.all.return_value = [game_mock]

        result = check_all_terminal_status(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "non-terminal" in result.message

    def test_no_games_passes_vacuously(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        result = check_all_terminal_status(session, _date(2026, 6, 24))
        assert result.passed is True


class TestCheckScoresPopulated:
    def test_all_have_scores(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_status = "COMPLETED"
        game_mock.home_score = 5
        game_mock.away_score = 3
        session.query.return_value.filter.return_value.all.return_value = [game_mock]

        result = check_scores_populated(session, _date(2026, 6, 24))
        assert result.passed is True

    def test_missing_scores_fails(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_status = "COMPLETED"
        game_mock.home_score = None
        game_mock.away_score = 3
        session.query.return_value.filter.return_value.all.return_value = [game_mock]

        result = check_scores_populated(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "missing scores" in result.message


class TestCheckChildStatsExist:
    def test_all_have_stats(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_id = "20260624LGSS0"
        session.query.return_value.filter.return_value.all.return_value = [game_mock]
        session.query.return_value.filter.return_value.scalar.side_effect = lambda: 1

        result = check_child_stats_exist(session, _date(2026, 6, 24))
        assert result.passed is True

    def test_missing_batting_fails(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_id = "20260624LGSS0"
        session.query.return_value.filter.return_value.all.return_value = [game_mock]
        session.query.return_value.filter.return_value.scalar.side_effect = lambda: 0

        result = check_child_stats_exist(session, _date(2026, 6, 24))
        assert result.passed is False


class TestCheckNoNullPlayerIds:
    def test_no_nulls_passes(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_id = "20260624LGSS0"
        session.query.return_value.filter.return_value.all.return_value = [game_mock]
        session.query.return_value.filter.return_value.scalar.side_effect = lambda: 0

        result = check_no_null_player_ids(session, _date(2026, 6, 24))
        assert result.passed is True

    def test_null_ids_fails(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_id = "20260624LGSS0"
        session.query.return_value.filter.return_value.all.return_value = [game_mock]
        session.query.return_value.filter.return_value.scalar.side_effect = lambda: 3

        result = check_no_null_player_ids(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "NULL player_id" in result.message


class TestCheckDuplicateGames:
    def _game(self, game_id: str) -> MagicMock:
        game = MagicMock()
        game.game_id = game_id
        return game

    def test_doubleheader_slots_are_not_duplicate_games(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            self._game("20260624LGSS0"),
            self._game("20260624LGSS1"),
        ]

        result = check_duplicate_games(session, _date(2026, 6, 24))

        assert result.passed is True

    def test_legacy_and_modern_aliases_for_same_slot_are_duplicates(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = [
            self._game("20260418SSGNC0"),
            self._game("20260418SKNC0"),
        ]

        result = check_duplicate_games(session, _date(2026, 4, 18))

        assert result.passed is False
        assert result.details["duplicates"][0]["canonical_slot"] == "20260418SKNC0"


class TestRunIntegrityChecks:
    def test_all_pass(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 5
        session.query.return_value.filter.return_value.all.return_value = []

        with patch.object(checker_module, "SessionLocal") as mock_local:
            mock_local.return_value.__enter__.return_value = session
            mock_local.return_value.__exit__.return_value = False

            report = run_integrity_checks("20260624")

        assert isinstance(report, IntegrityReport)
        assert report.target_date == "20260624"
        assert report.total_checks > 0

    def test_invalid_date_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid date format"):
            run_integrity_checks("invalid")


class TestMain:
    def test_success_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        report = IntegrityReport(
            target_date="20260624",
            timestamp_kst="2026-06-24T01:00:00+09:00",
            total_checks=6,
            passed_checks=6,
            failed_checks=0,
            results=[],
            overall_passed=True,
        )

        with patch.object(checker_module, "run_integrity_checks", return_value=report):
            with pytest.raises(SystemExit) as exc_info:
                main(["--date", "20260624"])
            assert exc_info.value.code == 0

    def test_failure_exits_one(self) -> None:
        report = IntegrityReport(
            target_date="20260624",
            timestamp_kst="2026-06-24T01:00:00+09:00",
            total_checks=6,
            passed_checks=4,
            failed_checks=2,
            results=[],
            overall_passed=False,
        )

        with patch.object(checker_module, "run_integrity_checks", return_value=report):
            with pytest.raises(SystemExit) as exc_info:
                main(["--date", "20260624"])
            assert exc_info.value.code == 1

    def test_invalid_date_exits_one(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["--date", "bad"])
        assert exc_info.value.code == 1

    def test_json_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        report = IntegrityReport(
            target_date="20260624",
            timestamp_kst="2026-06-24T01:00:00+09:00",
            total_checks=6,
            passed_checks=6,
            failed_checks=0,
            results=[
                CheckResult(name="test_check", passed=True, message="ok"),
            ],
            overall_passed=True,
        )

        with patch.object(checker_module, "run_integrity_checks", return_value=report):
            with pytest.raises(SystemExit):
                main(["--date", "20260624", "--json"])

        capsys.readouterr()


def _date(year: int, month: int, day: int) -> Any:
    from datetime import date

    return date(year, month, day)


class TestCheckFuturesDailyIntegrity:
    def test_no_records_passes(self) -> None:
        session = MagicMock()
        # Mock empty queries for batting and pitching
        session.query.return_value.filter.return_value.all.side_effect = [[], []]

        result = check_futures_daily_integrity(session, _date(2026, 6, 24))
        assert result.passed is True
        assert "No Futures records updated" in result.message

    def test_impossible_batting_stat_fails(self) -> None:
        session = MagicMock()

        # Mock batting record with AB > PA
        bat_record = MagicMock()
        bat_record.player_id = 999
        bat_record.plate_appearances = 5
        bat_record.at_bats = 10
        bat_record.hits = 2
        bat_record.doubles = 0
        bat_record.triples = 0
        bat_record.home_runs = 0
        bat_record.strikeouts = 0
        bat_record.walks = 0
        bat_record.hbp = 0
        bat_record.sacrifice_flies = 0
        bat_record.avg = 0.200
        bat_record.obp = 0.200
        bat_record.slg = 0.200
        bat_record.extra_stats = None

        # Return mock batting record and empty pitching list
        session.query.return_value.filter.return_value.all.side_effect = [[bat_record], []]

        result = check_futures_daily_integrity(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "Player 999 Batting: Impossible stats (PA=5, AB=10" in result.details["errors"][0]
