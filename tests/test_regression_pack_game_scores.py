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


def _game(
    conn, game_id: str, *, home: int | None, away: int | None, status: str = "COMPLETED", primary: bool = True
) -> None:
    conn.execute(
        text(
            "INSERT INTO game (game_id, game_date, game_status, home_score, away_score, is_primary)"
            " VALUES (:g, '2026-08-20', :s, :h, :a, :p)"
        ),
        {"g": game_id, "s": status, "h": home, "a": away, "p": 1 if primary else 0},
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
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

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
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "pass"

    def test_one_missing_side_is_reported(self, conn) -> None:
        """Half a score is still a gap: the missing half has the same evidence."""
        _game(conn, "20260820HTHH0", home=6, away=None)
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

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
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

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

    def test_a_short_inning_line_is_not_evidence_of_a_missing_total(self, conn) -> None:
        """The innings have to look like a whole game before they prove anything.

        A partial line sums to a partial score, so it does not say what the final
        total was. Counting the game here would claim evidence the rows do not
        carry -- exactly one stored game is in this shape (20230404LTSK0, seven
        innings on each side) and its innings are the defect, not its score.
        """
        _game(conn, "20230404LTSK0", home=None, away=None)
        _innings(conn, "20230404LTSK0", home=[1, 0, 0, 1, 1, 0, 0], away=[1, 0, 0, 0, 0, 0, 0])

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "pass", "a seven-inning line was treated as proof of a final total"

    def test_a_home_line_one_shorter_than_away_is_still_a_whole_game(self, conn) -> None:
        """The home side does not bat the bottom ninth when it already leads.

        This is 164 of the 365 stored games, so a rule that demanded nine on both
        sides would quietly stop measuring most of what this check is for.
        """
        _game(conn, "20260820HTHH0", home=None, away=None)
        _innings(
            conn,
            "20260820HTHH0",
            home=[2, 2, 2, 2, 1, 1, 0, 0],
            away=[1, 1, 1, 1, 1, 0, 0, 0, 0],
        )

        result = _result(conn, "game_score_missing_with_innings")

        assert result.status == "fail", "a game that ended after the top of the ninth was not measured"
        assert result.violation_count == 1


class TestTheScoreAndTheInningsDisagree:
    def test_a_score_that_disagrees_with_its_innings_is_reported(self, conn) -> None:
        """The other direction, and the reason the save path only fills a blank.

        Filling a blank is arithmetic. Overwriting a stated total would be a
        judgement, and it would erase the only trace that the two parts of the
        source page disagreed. Both shapes have to be visible.
        """
        _game(conn, "20260820HTHH0", home=9, away=8)
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

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
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

        result = _result(conn, "game_score_disagrees_with_innings")

        assert result.status == "fail", "a disagreement in the away total was not reported"
        assert result.violation_count == 1

    def test_a_disagreement_in_the_home_total_alone_is_reported(self, conn) -> None:
        """The other direction, so neither comparison can be dropped."""
        _game(conn, "20260820HTHH0", home=7, away=10)
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

        result = _result(conn, "game_score_disagrees_with_innings")

        assert result.status == "fail"
        assert result.violation_count == 1

    def test_agreement_is_not_reported(self, conn) -> None:
        _game(conn, "20260820HTHH0", home=6, away=10)
        _innings(conn, "20260820HTHH0", home=[1, 1, 1, 1, 1, 1, 0, 0, 0], away=[2, 2, 2, 1, 1, 1, 1, 0, 0])

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


class TestAFinishedGameCannotBeShorterThanTheMinimum:
    """Five is the league's own floor for a result, so below it is impossible.

    The check exists because the database cannot say what a no-game is.
    ``20260830LGLT0`` was declared void after three innings of rain, is stored
    COMPLETED, and carries the partial 0-2 as a result -- and nothing upstream
    can stop the next one, because neither the status vocabulary nor the
    schedule parser knows the verdict exists. Every case below is about the
    shape being impossible rather than about any particular score, because that
    is the only property the database retains after the verdict is lost.
    """

    def test_a_three_inning_result_is_reported(self, conn) -> None:
        """The stored row's shape: a result out of an inning line that cannot hold one."""
        _game(conn, "20260830LGLT0", home=0, away=2)
        _innings(conn, "20260830LGLT0", away=[0, 0, 0], home=[0, 0, 0])

        result = _result(conn, "game_final_with_too_few_innings")

        assert result is not None, "no check reports a finished game shorter than any real one"
        assert result.status == "fail"
        assert result.violation_count == 1
        assert result.sample_ids == ("20260830LGLT0",)

    def test_four_innings_is_still_too_short(self, conn) -> None:
        """The boundary below the floor, so the number is a tested edge not a guess."""
        _game(conn, "20260830FOUR", home=3, away=4)
        _innings(conn, "20260830FOUR", away=[0, 0, 0, 0], home=[0, 0, 0, 0])

        result = _result(conn, "game_final_with_too_few_innings")

        assert result.status == "fail"
        assert result.violation_count == 1

    @pytest.mark.parametrize(
        ("away_innings", "home_innings"),
        [
            (5, 5),
            (6, 5),
            (8, 7),
            (9, 9),
            (9, 8),
        ],
        ids=["floor", "home-one-behind", "called-short", "regulation", "no-bottom-ninth"],
    )
    def test_every_game_reaching_the_floor_is_a_whole_game(self, conn, away_innings, home_innings) -> None:
        """23 stored games are called between five and eight innings.

        A threshold of nine would report all of them as defects, and those are
        real results. Weather can end a game from the fifth onward, so the floor
        has to be the point below which no result exists, not the length of the
        common case.
        """
        _game(conn, "20260830REAL", home=1, away=2)
        _innings(conn, "20260830REAL", away=[0] * away_innings, home=[0] * home_innings)

        result = _result(conn, "game_final_with_too_few_innings")

        assert result.status == "pass", f"{away_innings}/{home_innings} innings were reported as impossible"

    def test_a_home_side_further_than_one_behind_is_reported(self, conn) -> None:
        """The floor alone misses a long line whose halves do not match.

        The away side always bats at least as many innings as the home side and
        the home side at most one fewer, so six away against three home cannot
        be a real game however long it reads. No stored game has this shape, so
        without this branch the difference clause would have no test at all.
        """
        _game(conn, "20260830MISMATCH", home=3, away=6)
        _innings(conn, "20260830MISMATCH", away=[0, 0, 0, 0, 0, 0], home=[0, 0, 0])

        result = _result(conn, "game_final_with_too_few_innings")

        assert result.status == "fail"
        assert result.violation_count == 1

    def test_a_cancelled_game_with_a_short_line_is_not_a_result(self, conn) -> None:
        """A void game has no result, so it cannot have too few innings for one.

        This is the state ``repair_no_game_declared`` writes: the row keeps its
        played innings and loses its status as a result. If the check counted it,
        the repair itself would leave the gate red and the check would be
        reporting its own fix.
        """
        _game(conn, "20260830LGLT0", home=None, away=None, status="CANCELLED")
        _innings(conn, "20260830LGLT0", away=[0, 0, 0], home=[0, 0, 0])

        result = _result(conn, "game_final_with_too_few_innings")

        assert result.status == "pass", "a voided game was still counted as having a result"

    def test_a_game_that_has_not_been_played_is_not_reported(self, conn) -> None:
        _game(conn, "20260830FUTURE", home=None, away=None, status="SCHEDULED")

        assert _result(conn, "game_final_with_too_few_innings").status == "pass"

    def test_a_game_with_no_innings_belongs_to_a_different_check(self, conn) -> None:
        """Counting it here too would report the same rows under two headings.

        ``game_result_without_body`` already covers a stored result whose body
        was never collected. The two are different facts: one is a result with
        no evidence, the other is evidence that cannot hold a result.
        """
        _game(conn, "20260820HTHH0", home=6, away=10)

        assert _result(conn, "game_final_with_too_few_innings").status == "pass"

    def test_one_sided_innings_are_not_a_short_game(self, conn) -> None:
        """Both sides must be present; one side's absence is a partial crawl."""
        _game(conn, "20260830ONE", home=0, away=0)
        _innings(conn, "20260830ONE", away=[0, 0, 0], home=[])

        assert _result(conn, "game_final_with_too_few_innings").status == "pass"

    def test_a_non_primary_row_is_someone_elses_version(self, conn) -> None:
        """A duplicated or superseded row is not the game the gate describes.

        Every check in this pack reads ``is_primary`` first, and dropping it
        would count the same game once per copy rather than once. No stored
        duplicate is short, so this branch has no production row to fall back on
        and only a test can hold it.
        """
        _game(conn, "20260830DUP", home=0, away=2, primary=False)
        _innings(conn, "20260830DUP", away=[0, 0, 0], home=[0, 0, 0])

        assert _result(conn, "game_final_with_too_few_innings").status == "pass"
