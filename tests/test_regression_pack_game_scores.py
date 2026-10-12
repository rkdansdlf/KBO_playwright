"""A game with inning lines and no score is one the gate should be able to see.

On 2026-08-20 five games were stored COMPLETED with both team totals missing,
each with a complete set of inning lines -- away 10 home 6, away 16 home 4, and
so on. The totals were already written, once per inning, in the next table.

``audit_historical_invariants`` states the rule (``home_score ==
SUM(runs for home)``) but is a manual 1982-2000 script, so it says nothing about
2026. The daily pack had no check for it, which is why a batch that filled the
innings on 2026-08-26 and skipped the totals was recorded as a success.

These fix that gap by checking the invariant the script already names.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from src.validators.data_quality_regression_pack import run_regression_pack


@pytest.fixture
def conn():
    """The two tables the invariant spans, with only the columns it reads."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as setup:
        setup.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY,
                    game_date DATE,
                    game_status TEXT,
                    home_score INTEGER,
                    away_score INTEGER,
                    is_primary BOOLEAN
                )
                """,
            ),
        )
        setup.execute(
            text(
                """
                CREATE TABLE game_inning_scores (
                    game_id TEXT,
                    inning INTEGER,
                    team_side TEXT,
                    runs INTEGER
                )
                """,
            ),
        )
    with engine.connect() as c:
        yield c


def _game(conn, game_id: str, *, home: int | None, away: int | None, status: str = "COMPLETED") -> None:
    conn.execute(
        text(
            "INSERT INTO game (game_id, game_date, game_status, home_score, away_score, is_primary)"
            " VALUES (:g, '2026-08-20', :s, :h, :a, 1)"
        ),
        {"g": game_id, "s": status, "h": home, "a": away},
    )


def _innings(conn, game_id: str, *, home: list[int], away: list[int]) -> None:
    for side, runs in (("home", home), ("away", away)):
        for number, value in enumerate(runs, start=1):
            conn.execute(
                text("INSERT INTO game_inning_scores (game_id, inning, team_side, runs) VALUES (:g, :i, :s, :r)"),
                {"g": game_id, "i": number, "s": side, "r": value},
            )


def _result(conn, check_id: str):
    report = run_regression_pack(conn)
    return next((r for r in report.results if r.check_id == check_id), None)


class TestAScoreMissingBesideItsInnings:
    def test_a_game_with_innings_and_no_score_is_reported(self, conn) -> None:
        """The 2026-08-20 shape: everything present except the two totals."""
        _game(conn, "20260820HTHH0", home=None, away=None)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        result = _result(conn, "game_score_missing_with_innings")

        assert result is not None, "no check reports a stored score that was never filled"
        assert result.status == "fail"
        assert result.violation_count == 1
        assert result.sample_ids == ("20260820HTHH0",)

    def test_the_game_can_be_said_to_have_played(self, conn) -> None:
        """A scheduled game with no innings is not a gap; only one that has them is.

        Every real game has innings. A row with innings and no total is a row
        whose total was missed, which is different in kind from a row that has
        not been crawled yet.
        """
        _game(conn, "20260825HTHH0", home=None, away=None, status="SCHEDULED")

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "pass", "a game that has not been played was reported as missing a score"

    def test_a_complete_game_is_not_reported(self, conn) -> None:
        _game(conn, "20260820HTHH0", home=6, away=10)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "pass"

    def test_one_missing_side_is_reported(self, conn) -> None:
        """Half a score is still a gap: the missing half has the same evidence."""
        _game(conn, "20260820HTHH0", home=6, away=None)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "fail"
        assert result.violation_count == 1

    def test_a_game_with_no_innings_is_not_this_check(self, conn) -> None:
        """Without innings there is no evidence of a score that should exist.

        Reporting it would fire on every scheduled game the crawler has not
        reached, which is the same as not reporting anything.
        """
        _game(conn, "20260825HTHH0", home=None, away=None)

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "pass"

    def test_a_cancelled_game_is_not_reported(self, conn) -> None:
        """A cancelled game legitimately has no total, even with innings recorded."""
        _game(conn, "20260820HTHH0", home=None, away=None, status="CANCELLED")
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "pass"

    def test_one_sided_innings_are_not_enough_to_call_a_total_missing(self, conn) -> None:
        """Half an inning line cannot say a total should exist.

        This is the case that was missing: every other test in this file gives
        both sides, so dropping the both-sides condition from the SQL passed the
        whole file. One stored game has innings for a single side out of 12,008
        with innings, and reading it as a missed total would invent a shutout for
        the side that was never recorded.
        """
        _game(conn, "20260820HTHH0", home=None, away=None)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[])

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "pass", "half an inning line was treated as a total that should exist"


class TestTheScoreAndTheInningsDisagree:
    def test_a_score_that_disagrees_with_its_innings_is_reported(self, conn) -> None:
        """The other direction, and the reason the save path only fills a blank.

        Filling a blank is arithmetic. Overwriting a stated total would be a
        judgement, and it would erase the only trace that the two parts of the
        source page disagreed. Both shapes have to be visible.
        """
        _game(conn, "20260820HTHH0", home=9, away=8)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        result = _result(conn, "game_score_disagrees_with_innings")

        assert result is not None, "a score that contradicts its own innings is not reported anywhere"
        assert result.status == "fail"
        assert result.violation_count == 1

    def test_a_disagreement_in_the_away_total_alone_is_reported(self, conn) -> None:
        """Both directions, separately.

        With only a home-side disagreement in the file, dropping the away
        comparison from the SQL left every test passing -- the check would have
        reported home disagreements and silently ignored away ones. A stored
        disagreement can be on either side.
        """
        _game(conn, "20260820HTHH0", home=6, away=7)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        result = _result(conn, "game_score_disagrees_with_innings")

        assert result.status == "fail", "a disagreement in the away total was not reported"
        assert result.violation_count == 1

    def test_a_disagreement_in_the_home_total_alone_is_reported(self, conn) -> None:
        """The other direction, so neither comparison can be dropped."""
        _game(conn, "20260820HTHH0", home=7, away=10)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        result = _result(conn, "game_score_disagrees_with_innings")

        assert result.status == "fail"
        assert result.violation_count == 1

    def test_agreement_is_not_reported(self, conn) -> None:
        _game(conn, "20260820HTHH0", home=6, away=10)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[5, 2, 1, 2])

        assert _result(conn, "game_score_disagrees_with_innings").status == "pass"

    def test_a_game_with_no_innings_is_not_judged(self, conn) -> None:
        """Nothing to disagree with."""
        _game(conn, "20260820HTHH0", home=6, away=10)

        assert _result(conn, "game_score_disagrees_with_innings").status == "pass"

    def test_one_sided_innings_do_not_make_a_disagreement(self, conn) -> None:
        """One side's innings cannot contradict a total it never covered.

        The 2026-08-26 rows have both sides, so this is about the shape rather
        than about any stored game -- but filling a score from half an inning
        line is exactly the mistake the save path refuses to make.
        """
        _game(conn, "20260820HTHH0", home=6, away=None)
        _innings(conn, "20260820HTHH0", home=[3, 1, 0, 2], away=[])

        assert _result(conn, "game_score_disagrees_with_innings").status == "pass"
