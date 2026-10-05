"""Tests for data integrity checker."""

from __future__ import annotations

import json
import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import oracle

from src.cli import data_integrity_checker as checker_module
from src.cli.data_integrity_checker import (
    EXPECT_GAMES_ENV,
    CheckResult,
    IntegrityReport,
    _games_expectation_override,
    _primary_game_predicate,
    _target_date_predicate,
    check_all_terminal_status,
    check_child_stats_exist,
    check_duplicate_games,
    check_futures_daily_integrity,
    check_game_status_populated,
    check_games_exist,
    check_no_null_player_ids,
    check_pa_formula_integrity,
    check_scores_populated,
    check_season_stat_team_code,
    games_are_expected,
    main,
    run_integrity_checks,
)
from src.models.game import Game
from src.models.player import PlayerSeasonBatting


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


def _oracle_session() -> MagicMock:
    session = MagicMock()
    session.bind.dialect.name = "oracle"
    return session


def test_oracle_primary_game_predicate_avoids_is_numeric_boolean() -> None:
    sql = str(
        select(Game)
        .where(_primary_game_predicate(_oracle_session(), Game.is_primary))
        .compile(dialect=oracle.dialect()),
    )

    assert " IS 1" not in sql
    assert "game.is_primary =" in sql


def test_oracle_target_date_predicate_uses_trunc_for_timestamps() -> None:
    sql = str(
        select(PlayerSeasonBatting)
        .where(_target_date_predicate(_oracle_session(), PlayerSeasonBatting.updated_at, _date(2026, 8, 18)))
        .compile(dialect=oracle.dialect()),
    )

    assert "TRUNC(" in sql.upper()


class TestCheckGamesExist:
    def test_no_games_passes_as_rest_day_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Rest days and the off-season must not raise a permanent false alarm."""
        monkeypatch.delenv(EXPECT_GAMES_ENV, raising=False)
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 0

        result = check_games_exist(session, _date(2026, 6, 24))
        assert result.passed is True
        assert result.details["count"] == 0
        assert result.details["enforced"] is False
        assert result.details["games_expected"] is False
        assert EXPECT_GAMES_ENV in result.message

    def test_no_games_fails_when_games_are_expected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXPECT_GAMES_ENV, "1")
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 0

        result = check_games_exist(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "No game rows found" in result.message
        assert result.details["enforced"] is True

    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", "yes", "on", " 1 "])
    def test_truthy_env_values_enforce(self, monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
        monkeypatch.setenv(EXPECT_GAMES_ENV, raw)
        assert games_are_expected() is True

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", "", "bogus"])
    def test_falsy_env_values_do_not_enforce(self, monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
        monkeypatch.setenv(EXPECT_GAMES_ENV, raw)
        assert games_are_expected() is False

    def test_games_exist_passes(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 5

        result = check_games_exist(session, _date(2026, 6, 24))
        assert result.passed is True
        assert "Found 5 game(s)" in result.message
        assert result.details["games_expected"] is True


class TestGamesExpectationOverride:
    @pytest.mark.parametrize(
        ("initial", "expect"),
        [(None, True), ("1", False), ("0", True), ("bogus", True)],
    )
    def test_environment_is_restored(
        self,
        monkeypatch: pytest.MonkeyPatch,
        initial: str | None,
        expect: bool,
    ) -> None:
        if initial is None:
            monkeypatch.delenv(EXPECT_GAMES_ENV, raising=False)
        else:
            monkeypatch.setenv(EXPECT_GAMES_ENV, initial)

        with _games_expectation_override(expect_games=expect):
            assert games_are_expected() is expect

        assert os.environ.get(EXPECT_GAMES_ENV) == initial

    def test_environment_is_restored_when_the_body_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(EXPECT_GAMES_ENV, raising=False)

        with pytest.raises(RuntimeError, match="boom"), _games_expectation_override(expect_games=True):
            msg = "boom"
            raise RuntimeError(msg)

        assert EXPECT_GAMES_ENV not in os.environ


class TestExpectGamesFlags:
    def test_no_flag_leaves_the_operator_decision_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Passing no flag must mean "unspecified", never "do not expect games".

        The override used to be computed as ``args.expect_games and not
        args.allow_no_games``, which is ``False`` when neither flag is given. That
        collapsed "the operator said nothing" into "the operator said no games",
        so an environment of ``INTEGRITY_EXPECT_GAMES=1`` was silently
        overwritten and the documented escalation path stopped working from the
        command line.
        """
        monkeypatch.setenv(EXPECT_GAMES_ENV, "1")
        seen: list[bool] = []
        _run_and_record_expectation(seen, checker_module, monkeypatch, ["--date", "20260925"])

        assert seen == [True]

    def test_environment_is_untouched_when_no_flag_is_given(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXPECT_GAMES_ENV, "1")
        seen: list[bool] = []
        _run_and_record_expectation(seen, checker_module, monkeypatch, ["--date", "20260925"])

        assert os.environ[EXPECT_GAMES_ENV] == "1"

    def test_conflicting_flags_are_rejected(self) -> None:
        """Silently ignoring one of two opposite flags hides a typo'd invocation.

        Both flags name the same decision, so asking for the strict gate and the
        permissive one in a single command has no answer. Picking one by
        precedence means an operator who misspelled the other never learns their
        check ran under the opposite rule.
        """
        args = checker_module.build_arg_parser().parse_args(["--date", "20260925", "--expect-games"])

        with pytest.raises(SystemExit) as exc:
            checker_module.main(["--date", "20260925", "--expect-games", "--allow-no-games"])

        assert exc.value.code == 2
        assert args.expect_games is True

    def test_expect_games_flag_reaches_the_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The CLI flag must actually tighten the gate, not just parse."""
        monkeypatch.delenv(EXPECT_GAMES_ENV, raising=False)
        seen: list[bool] = []

        def _fake_run(target_date: str) -> IntegrityReport:
            seen.append(games_are_expected())
            return IntegrityReport(
                target_date=target_date,
                timestamp_kst="2026-09-26T00:00:00+09:00",
                total_checks=1,
                passed_checks=1,
                failed_checks=0,
                results=[CheckResult(name="games_exist", passed=True, message="ok")],
                overall_passed=True,
            )

        with (
            patch.object(checker_module, "run_integrity_checks", side_effect=_fake_run),
            pytest.raises(SystemExit) as exc,
        ):
            checker_module.main(["--date", "20260925", "--expect-games"])

        assert exc.value.code == 0
        assert seen == [True]
        # The override must not leak into the process environment.
        assert EXPECT_GAMES_ENV not in os.environ

    def test_allow_no_games_flag_overrides_a_strict_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(EXPECT_GAMES_ENV, "1")
        seen: list[bool] = []
        _run_and_record_expectation(seen, checker_module, monkeypatch, ["--date", "20260925", "--allow-no-games"])

        assert seen == [False]
        assert os.environ[EXPECT_GAMES_ENV] == "1"


def _run_and_record_expectation(
    seen: list[bool],
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
) -> None:
    """Run ``main`` over a stubbed report and record the expectation it saw."""
    del monkeypatch  # the caller already arranged the environment

    def _fake_run(target_date: str) -> IntegrityReport:
        seen.append(games_are_expected())
        return IntegrityReport(
            target_date=target_date,
            timestamp_kst="2026-09-26T00:00:00+09:00",
            total_checks=1,
            passed_checks=1,
            failed_checks=0,
            results=[CheckResult(name="games_exist", passed=True, message="ok")],
            overall_passed=True,
        )

    with (
        patch.object(module, "run_integrity_checks", side_effect=_fake_run),
        pytest.raises(SystemExit) as exc,
    ):
        module.main(argv)

    assert exc.value.code == 0


class TestGamesExistDetailsContract:
    def test_rest_day_records_that_nothing_was_expected(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 0

        with patch.object(checker_module, "games_are_expected", return_value=False):
            result = check_games_exist(session, _date(2026, 6, 24))

        assert result.details == {"count": 0, "enforced": False, "games_expected": False}

    def test_present_games_report_the_rows_not_the_environment(self) -> None:
        """``enforced`` describes this run's decision, not a second environment read.

        The passing path re-read the environment to fill ``enforced``, so the key
        meant "were games required" on one path and "was this run strict" on the
        other. A consumer reading the key cannot tell which, and the value could
        disagree with the verdict already returned if the environment shifted
        mid-check.
        """
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 5

        with patch.object(checker_module, "games_are_expected", return_value=False):
            result = check_games_exist(session, _date(2026, 6, 24))

        assert result.passed is True
        assert result.details == {"count": 5, "enforced": False, "games_expected": True}

    def test_expectation_is_read_once_per_check(self) -> None:
        """One read per check, so the reported decision is the decided one."""
        session = MagicMock()
        session.query.return_value.filter.return_value.count.return_value = 5

        with patch.object(checker_module, "games_are_expected", return_value=True) as read:
            check_games_exist(session, _date(2026, 6, 24))

        assert read.call_count == 1


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
        session.execute.return_value.fetchall.return_value = [(0, 0, 0), (0, 0, 0)]

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


class TestCheckSeasonStatTeamCode:
    def test_source_limited_rows_pass_without_team_code_mutation(self) -> None:
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [(3, 3, 0), (2, 0, 2)]

        result = check_season_stat_team_code(session)

        assert result.passed is True
        assert result.details == {
            "batting_null": 3,
            "pitching_null": 2,
            "batting_source_limited": 3,
            "pitching_source_limited": 2,
            "source_limited": 5,
            "unresolved": 0,
            "total_null": 5,
        }

    def test_unclassified_rows_fail(self) -> None:
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = [(3, 1, 1), (2, 0, 1)]

        result = check_season_stat_team_code(session)

        assert result.passed is False
        assert result.message == "2 season stats have unresolved team_code gaps"
        assert result.details["unresolved"] == 2


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


class TestCheckPaFormulaIntegrity:
    def test_no_games_passes(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        result = check_pa_formula_integrity(session, _date(2026, 6, 24))
        assert result.passed is True
        assert "vacuously true" in result.message

    def test_valid_pa_formula_passes(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_id = "20260624LGSS0"
        query1 = MagicMock()
        query1.filter.return_value.all.return_value = [game_mock]
        query2 = MagicMock()
        query2.filter.return_value.all.return_value = []
        session.query.side_effect = [query1, query2]

        result = check_pa_formula_integrity(session, _date(2026, 6, 24))
        assert result.passed is True
        assert "satisfy PA formula" in result.message

    def test_violating_pa_formula_fails(self) -> None:
        session = MagicMock()
        game_mock = MagicMock()
        game_mock.game_id = "20260624LGSS0"

        stat_mock = MagicMock()
        stat_mock.game_id = "20260624LGSS0"
        stat_mock.plate_appearances = 4
        stat_mock.at_bats = 3
        stat_mock.walks = 0
        stat_mock.hbp = 0
        stat_mock.sacrifice_hits = 0
        stat_mock.sacrifice_flies = 0

        query1 = MagicMock()
        query1.filter.return_value.all.return_value = [game_mock]
        query2 = MagicMock()
        query2.filter.return_value.all.return_value = [stat_mock]
        session.query.side_effect = [query1, query2]

        result = check_pa_formula_integrity(session, _date(2026, 6, 24))
        assert result.passed is False
        assert "PA formula violation" in result.message
        assert result.details["violations"] == 1
