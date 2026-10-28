"""A cancelled game the relay finished is a shape no check could see.

``20261026HTSS0`` carried a complete 9-inning game -- 582 play-by-play rows, the
feed's closing marker written twice, and the running score in its at-bats ending
at KIA 5 : 삼성 1 -- while ``game_status`` said CANCELLED with both score columns
NULL. The row reached that state before the game started (``refresh_game_status``
wrote it at 18:01), and the cancellation then removed the game from the detail
targets, so the boxscore that could have corrected it was never fetched.

Neither neighbour could report it. ``game_cancelled_with_scores`` needs both
scores and this game has none; ``game_result_without_body`` reads a result and
there is no stored result. Every gate reads the status column, which is the one
column that was wrong.

These fix that gap by asking the relay's own evidence instead.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from src.constants import FINAL_PBP_MARKER
from src.validators.data_quality_regression_pack import run_regression_pack

CHECK_ID = "game_cancelled_with_finished_relay"


@pytest.fixture
def conn():
    """The two tables the check spans, with only the columns it reads."""
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
        setup.execute(text("CREATE TABLE game_play_by_play (game_id TEXT, play_description TEXT)"))
    with engine.connect() as c:
        yield c


def _game(
    conn,
    game_id: str,
    *,
    status: str = "CANCELLED",
    home: int | None = None,
    away: int | None = None,
    primary: bool = True,
) -> None:
    conn.execute(
        text(
            "INSERT INTO game (game_id, game_date, game_status, home_score, away_score, is_primary)"
            " VALUES (:g, '2026-10-26', :s, :h, :a, :p)",
        ),
        {"g": game_id, "s": status, "h": home, "a": away, "p": 1 if primary else 0},
    )


def _pbp(conn, game_id: str, description: str) -> None:
    conn.execute(
        text("INSERT INTO game_play_by_play (game_id, play_description) VALUES (:g, :d)"),
        {"g": game_id, "d": description},
    )


def _result(conn):
    report = run_regression_pack(conn)
    return next((r for r in report.results if r.check_id == CHECK_ID), None)


class TestACancelledGameTheRelayFinished:
    def test_the_shape_is_reported(self, conn) -> None:
        """The 10-26 shape: no score, no boxscore, and a feed that closed the game."""
        _game(conn, "20261026HTSS0")
        _pbp(conn, "20261026HTSS0", FINAL_PBP_MARKER)

        result = _result(conn)

        assert result is not None, "no check reads the relay's evidence about a cancelled game"
        assert result.status == "warn", "the grade is pinned below, with the reason it is not fail"
        assert result.violation_count == 1
        assert result.sample_ids == ("20261026HTSS0",)

    def test_a_relay_that_stopped_early_is_not_evidence(self, conn) -> None:
        """A game that stopped mid-inning was not carried to its closing marker."""
        _game(conn, "20261026HTSS0")
        _pbp(conn, "20261026HTSS0", "박정우 : 삼진 아웃")

        assert _result(conn).status == "pass"

    def test_the_marker_inside_a_longer_row_still_counts(self, conn) -> None:
        """The feed writes the marker as its own row; the check reads it anywhere."""
        _game(conn, "20261026HTSS0")
        _pbp(conn, "20261026HTSS0", f"경기 종료 {FINAL_PBP_MARKER}")

        assert _result(conn).violation_count == 1

    def test_a_completed_game_is_not_reported(self, conn) -> None:
        """The repair's end state: same relay, a result stored beside it."""
        _game(conn, "20261026HTSS0", status="COMPLETED", home=1, away=5)
        _pbp(conn, "20261026HTSS0", FINAL_PBP_MARKER)

        assert _result(conn).status == "pass"

    def test_a_retired_duplicate_is_not_reported(self, conn) -> None:
        """Only what a reader sees is reported, the same rule the neighbours use."""
        _game(conn, "20261026HTSS0", primary=False)
        _pbp(conn, "20261026HTSS0", FINAL_PBP_MARKER)

        assert _result(conn).status == "pass"


class TestTheGradeIsDeliberate:
    def test_the_check_informs_rather_than_failing(self) -> None:
        """Five rows keep this from being ``fail``, and that has to stay true.

        2015-10-16 and the four 2021-05-08 games carry the same shape, and the
        historical write policy refuses to let anything correct them without an
        approval. Grading this ``fail`` would turn the daily gate red on the day
        it was added and leave it red -- which is how a gate stops being read.
        The count and the ids are still reported; the actionable path is the
        repair script, which names the rows it may and may not write.
        """
        from src.validators.data_quality_regression_pack import _CHECKS

        check = next(c for c in _CHECKS if c.check_id == CHECK_ID)

        assert check.informational is True


class TestTheCheckSaysWhenItCannotRun:
    def test_a_missing_relay_table_is_unmeasurable(self) -> None:
        """Without the table holding the evidence, say so rather than pass quietly."""
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
        with engine.connect() as conn:
            result = next(r for r in run_regression_pack(conn).results if r.check_id == CHECK_ID)

        assert result.status == "skipped"
        assert "game_play_by_play" in result.message

    def test_the_check_reads_the_published_marker(self) -> None:
        """A marker typed into the SQL keeps passing after the feed changes it.

        The check's whole job is to notice a feed that finished a game, so the row
        it looks for has to be the row the feed writes. Spelling the literal here
        means the two can drift: the SQL would look for a marker that no longer
        exists and report a clean database while every finished game went
        unreported -- the failure this check was added to stop.
        """
        from src.validators.data_quality_regression_pack import _CHECKS

        check = next(c for c in _CHECKS if c.check_id == CHECK_ID)

        assert FINAL_PBP_MARKER in check.count_sql
        assert FINAL_PBP_MARKER in check.sample_sql

    def test_a_partial_schema_can_be_required_to_fail(self, conn) -> None:
        """The daily gate asks for a real answer; a missing table is then a failure."""
        conn.execute(text("DROP TABLE game_play_by_play"))

        result = next(r for r in run_regression_pack(conn, require_schema=True).results if r.check_id == CHECK_ID)

        assert result.status == "fail"
        assert "game_play_by_play" in result.message
