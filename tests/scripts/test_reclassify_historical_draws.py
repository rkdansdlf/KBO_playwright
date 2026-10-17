"""Tests for the pre-2001 tied-game reclassification.

The correctness question is not "does it write DRAW" -- it is "does it write
DRAW to the right rows and nothing else". A run that also reclassified a
non-tied game, or a modern one, would destroy a real result while reporting
success, so most of what follows pins the edges of the selection rather than
the write itself.
"""

from __future__ import annotations

import json
import pathlib

import pytest
from sqlalchemy import create_engine, text

from scripts.maintenance.reclassify_historical_draws import (
    DRAW,
    SUBJECT_STATUSES,
    main,
    reclassify,
)


def _schema(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                game_date TEXT,
                home_team TEXT,
                away_team TEXT,
                home_score INTEGER,
                away_score INTEGER,
                game_status TEXT,
                is_primary BOOLEAN
            )
            """,
        ),
    )


def _insert(conn, rows: list[tuple]) -> None:
    for row in rows:
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, home_team, away_team,
                                  home_score, away_score, game_status, is_primary)
                VALUES (:game_id, :game_date, :home_team, :away_team,
                        :home_score, :away_score, :game_status, :is_primary)
                """,
            ),
            {
                "game_id": row[0],
                "game_date": row[1],
                "home_team": row[2],
                "away_team": row[3],
                "home_score": row[4],
                "away_score": row[5],
                "game_status": row[6],
                "is_primary": row[7],
            },
        )


class TestTheSelectionIsExactlyTheTiedUnresolvedGames:
    """Every excluded shape is a real game that must keep its status."""

    def test_a_tied_unresolved_game_becomes_a_candidate(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1)])
            conn.commit()

            report = reclassify(conn)
        finally:
            conn.close()

        assert report.matched == 1
        assert report.by_status == {"UNRESOLVED_MISSING": 1}

    def test_a_decided_game_is_left_alone(self) -> None:
        """A tie is the condition. A winner either way means this is not one."""
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(
                conn,
                [
                    ("19960624OBHT0", "1996-06-24", "HT", "OB", 4, 3, "UNRESOLVED_MISSING", 1),
                    ("19960625OBHT0", "1996-06-25", "HT", "OB", 1, 7, "UNRESOLVED_MISSING", 1),
                ],
            )
            conn.commit()

            report = reclassify(conn)
        finally:
            conn.close()

        assert report.matched == 0

    def test_a_modern_tied_game_is_left_alone(self) -> None:
        """2001 and later is crawled with bodies; the crawlers own those rows."""
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(conn, [("20240518LGHH0", "2024-05-18", "HH", "LG", 5, 5, "DRAW", 1)])
            conn.commit()

            report = reclassify(conn)
        finally:
            conn.close()

        assert report.matched == 0

    def test_a_retired_duplicate_is_not_touched(self) -> None:
        """``is_primary = FALSE`` rows are kept out of every count."""
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 0)])
            conn.commit()

            report = reclassify(conn)
        finally:
            conn.close()

        assert report.matched == 0

    def test_a_game_without_a_score_is_not_a_tie(self) -> None:
        """NULL is an unknown, not an equality. SQL would agree here, so pin it."""
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(
                conn,
                [
                    ("19960624OBHT0", "1996-06-24", "HT", "OB", None, None, "UNRESOLVED_MISSING", 1),
                    ("19960625OBHT0", "1996-06-25", "HT", "OB", 3, None, "UNRESOLVED_MISSING", 1),
                ],
            )
            conn.commit()

            report = reclassify(conn)
        finally:
            conn.close()

        assert report.matched == 0

    def test_an_already_draw_row_is_not_a_candidate(self) -> None:
        """Re-reporting it would make the count grow without changing anything."""
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "DRAW", 1)])
            conn.commit()

            report = reclassify(conn)
        finally:
            conn.close()

        assert report.matched == 0

    def test_a_season_filter_cannot_reach_the_next_season(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(
                conn,
                [
                    ("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1),
                    ("19970624OBHT0", "1997-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1),
                ],
            )
            conn.commit()

            report = reclassify(conn, season=1996)
        finally:
            conn.close()

        assert report.matched == 1
        assert report.by_season == {"1996": 1}

    def test_both_subject_statuses_are_selected(self) -> None:
        """The batch is not uniform: one pre-2001 row is stored as COMPLETED."""
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(
                conn,
                [
                    ("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1),
                    ("19980624OBHT0", "1998-06-24", "HT", "OB", 2, 2, "COMPLETED", 1),
                ],
            )
            conn.commit()

            report = reclassify(conn)
        finally:
            conn.close()

        assert report.by_status == {"UNRESOLVED_MISSING": 1, "COMPLETED": 1}


class TestTheDryRunWritesNothing:
    """A preview that writes is not a preview."""

    def test_dry_run_reports_without_updating(self, monkeypatch, tmp_path) -> None:
        import scripts.maintenance.reclassify_historical_draws as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1)])
            conn.commit()

            report = reclassify(conn)
            status = conn.execute(text("SELECT game_status FROM game")).scalar_one()
        finally:
            conn.close()

        assert report.matched == 1
        assert report.updated == 0
        assert status == "UNRESOLVED_MISSING"
        assert report.snapshot_path is None
        assert not list(tmp_path.iterdir())


class TestTheApplyWritesOnlyTheStatus:
    """The score, the teams and the identifier are not this script's business."""

    def test_the_status_changes_and_nothing_else(self, monkeypatch, tmp_path) -> None:
        import scripts.maintenance.reclassify_historical_draws as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1)])
            conn.commit()

            report = reclassify(conn, apply=True)
            row = conn.execute(
                text("SELECT game_date, home_team, away_team, home_score, away_score, game_status FROM game"),
            ).one()
        finally:
            conn.close()

        assert report.updated == 1
        assert row == ("1996-06-24", "HT", "OB", 3, 3, DRAW), "only the status may differ"

    def test_the_rollback_restores_each_row_its_own_status(self, monkeypatch, tmp_path) -> None:
        """One batched assignment would corrupt whichever half was the other status."""
        import scripts.maintenance.reclassify_historical_draws as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(
                conn,
                [
                    ("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1),
                    ("19980624OBHT0", "1998-06-24", "HT", "OB", 2, 2, "COMPLETED", 1),
                ],
            )
            conn.commit()

            report = reclassify(conn, apply=True)
        finally:
            conn.close()

        rollback = pathlib.Path(report.rollback_path).read_text(encoding="utf-8")

        # Replay it against a fresh copy of the pre-run state and compare.
        restored = create_engine("sqlite:///:memory:")
        restored_conn = restored.connect()
        try:
            _schema(restored_conn)
            _insert(
                restored_conn,
                [
                    ("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "DRAW", 1),
                    ("19980624OBHT0", "1998-06-24", "HT", "OB", 2, 2, "DRAW", 1),
                ],
            )
            for statement in rollback.splitlines():
                if statement.startswith("UPDATE"):
                    restored_conn.execute(text(statement))
            restored_conn.commit()
            statuses = dict(restored_conn.execute(text("SELECT game_id, game_status FROM game")).all())
        finally:
            restored_conn.close()

        assert statuses == {"19960624OBHT0": "UNRESOLVED_MISSING", "19980624OBHT0": "COMPLETED"}

    def test_the_snapshot_names_the_previous_status_of_every_row(self, monkeypatch, tmp_path) -> None:
        import scripts.maintenance.reclassify_historical_draws as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1)])
            conn.commit()

            report = reclassify(conn, apply=True)
        finally:
            conn.close()

        payload = json.loads(pathlib.Path(report.snapshot_path).read_text(encoding="utf-8"))
        assert payload["target_status"] == DRAW
        assert payload["previous"][0]["game_id"] == "19960624OBHT0"
        assert payload["previous"][0]["game_status"] == "UNRESOLVED_MISSING"


class TestTheEnvironmentGateIsDefaultDeny:
    """The write needs the flag and the variable, like every other restore."""

    def test_apply_without_the_variable_exits_three(self, monkeypatch, tmp_path) -> None:
        import scripts.maintenance.reclassify_historical_draws as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        monkeypatch.delenv(module.ENV_GATE, raising=False)
        db_path = tmp_path / "gate.db"
        url = f"sqlite:///{db_path}"
        engine = create_engine(url)
        with engine.begin() as conn:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1)])

        exit_code = main(["--apply", "--db-url", url])
        with engine.connect() as conn:
            status = conn.execute(text("SELECT game_status FROM game")).scalar_one()

        assert exit_code == 3
        assert status == "UNRESOLVED_MISSING", "a refused write must not reach the row"

    def test_apply_with_the_variable_proceeds(self, monkeypatch, tmp_path) -> None:
        import scripts.maintenance.reclassify_historical_draws as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setenv(module.ENV_GATE, "1")
        db_path = tmp_path / "allowed.db"
        url = f"sqlite:///{db_path}"
        engine = create_engine(url)
        with engine.begin() as conn:
            _schema(conn)
            _insert(conn, [("19960624OBHT0", "1996-06-24", "HT", "OB", 3, 3, "UNRESOLVED_MISSING", 1)])

        exit_code = main(["--apply", "--db-url", url])
        with engine.connect() as conn:
            status = conn.execute(text("SELECT game_status FROM game")).scalar_one()

        assert exit_code == 0
        assert status == DRAW

    def test_the_status_vocabulary_is_the_two_unresolved_shapes_only(self) -> None:
        """A third status here would widen the write without anything noticing."""
        assert set(SUBJECT_STATUSES) == {"UNRESOLVED_MISSING", "COMPLETED"}


def test_the_selection_sql_binds_every_value_it_uses() -> None:
    """Guards against a cutoff being interpolated back into the statement."""
    import inspect

    import scripts.maintenance.reclassify_historical_draws as module

    source = inspect.getsource(module._select_candidates)

    assert "HISTORICAL_CUTOFF" not in source.split("text(")[1].split(")")[0], "the cutoff must travel as a parameter"
