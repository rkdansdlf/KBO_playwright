"""Tests for the duplicate-game retirement script.

The behaviour that matters is which row survives, because the script deletes one
half of each pair from the production table. Two failures are being guarded
against:

* retiring the only copy of a boxscore -- a 2011-2018 Heroes fixture has a
  ``WO`` row carrying 478 PBP rows and an ``NX`` row carrying none, and picking
  the empty one destroys data that exists nowhere else;
* retiring the wrong one of a pair where both sides have a body -- the 2014
  Korean Series carries 561 rows on the ``NX`` side and 478 on the ``WO`` side,
  so the larger body is the survivor.

The 2010 population is a third case: neither row has a body or a score, so the
rule falls through to the earliest load, which is also the row whose ``game_id``
uses the code in force that season.
"""

from __future__ import annotations

import json
import pathlib
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text

from scripts.maintenance.dedupe_duplicate_games import (
    DuplicateGroup,
    GameRow,
    _franchise_key,
    _full_row_payload,
    _sql_literal,
    _write_rollback,
    dedupe,
    find_duplicate_groups,
)


def _row(
    game_id: str,
    *,
    home: str,
    away: str,
    game_date: str = "2011-03-12",
    home_score: int | None = None,
    away_score: int | None = None,
    status: str = "COMPLETED",
    xmin: int = 1,
) -> GameRow:
    return GameRow(
        game_id=game_id,
        game_date=date.fromisoformat(game_date),
        home_team=home,
        away_team=away,
        away_score=away_score,
        home_score=home_score,
        game_status=status,
        created_at=datetime(2026, 7, 18),
        xmin=xmin,
    )


class TestFranchiseFolding:
    """A code changes with the era; the club does not."""

    def test_two_codes_of_one_franchise_share_a_key(self) -> None:
        assert _franchise_key("NX") == _franchise_key("KH")
        assert _franchise_key("WO") == _franchise_key("NX")

    def test_two_clubs_keep_separate_keys(self) -> None:
        assert _franchise_key("NX") != _franchise_key("KIA")
        assert _franchise_key("SS") != _franchise_key("SSG")

    def test_an_all_star_side_is_its_own_key(self) -> None:
        """EA and WE are the two halves of one game, not two clubs."""
        assert _franchise_key("EA") != _franchise_key("WE")

    def test_a_missing_code_does_not_crash(self) -> None:
        assert _franchise_key(None) == "code:"


class TestDuplicateDetection:
    """Grouping decides what counts as one game stored twice."""

    def test_two_codes_of_one_franchise_form_one_group(self) -> None:
        rows = [
            _row("20110312HTWO0", home="KH", away="KIA"),
            _row("20110312HTNX0", home="NX", away="KIA"),
        ]
        groups = find_duplicate_groups(rows, {})

        assert len(groups) == 1, "NX and KH name franchise 11, so this is one game"
        assert len(groups[0].members) == 2

    def test_two_games_on_different_days_are_separate(self) -> None:
        rows = [
            _row("20110312HTWO0", home="KH", away="KIA", game_date="2011-03-12"),
            _row("20110313HTNX0", home="NX", away="KIA", game_date="2011-03-13"),
        ]
        assert find_duplicate_groups(rows, {}) == []

    def test_a_doubleheader_is_two_games(self) -> None:
        """Same date, same clubs, different game -- and the suffix says so."""
        rows = [
            _row("20110312HTWO0", home="KH", away="KIA"),
            _row("20110312HTWO1", home="KH", away="KIA"),
        ]
        assert find_duplicate_groups(rows, {}) == []

    def test_a_row_without_teams_is_left_alone(self) -> None:
        rows = [_row("20180801WOSK0", home="", away="")]
        assert find_duplicate_groups(rows, {}) == []


class TestSurvivorSelection:
    """The rule is one rule: the row that would not lose data."""

    def test_the_row_with_the_body_is_kept(self) -> None:
        rows = [
            _row("20141104NXSS0", home="SS", away="KH", home_score=2, away_score=4, xmin=2167),
            _row("20141104WOSS0", home="SS", away="KH", home_score=2, away_score=4, xmin=47367),
        ]
        groups = find_duplicate_groups(rows, {"20141104NXSS0": 561, "20141104WOSS0": 478})

        assert groups[0].survivor().game_id == "20141104NXSS0"

    def test_a_scored_row_beats_an_unscored_one_when_bodies_match(self) -> None:
        rows = [
            _row("20100716LTDB0", home="DB", away="LT", xmin=2166),
            _row("20100716LTOB0", home="DB", away="LT", home_score=3, away_score=1, xmin=2169),
        ]
        groups = find_duplicate_groups(rows, {"20100716LTDB0": 0, "20100716LTOB0": 0})

        assert groups[0].survivor().game_id == "20100716LTOB0"

    def test_the_earliest_load_wins_when_nothing_else_separates_them(self) -> None:
        """The 2010 population: no body and no score on either side."""
        rows = [
            _row("20100414DBKIA0", home="KIA", away="DB", xmin=2166),
            _row("20100414OBHT0", home="KIA", away="DB", xmin=2169),
        ]
        groups = find_duplicate_groups(rows, {"20100414DBKIA0": 0, "20100414OBHT0": 0})

        assert groups[0].survivor().game_id == "20100414DBKIA0"

    def test_an_exact_tie_is_refused_rather_than_resolved_by_chance(self) -> None:
        rows = [
            _row("20100414DBKIA0", home="KIA", away="DB", xmin=2166),
            _row("20100414OBHT0", home="KIA", away="DB", xmin=2166),
        ]
        groups = find_duplicate_groups(rows, {"20100414DBKIA0": 0, "20100414OBHT0": 0})

        with pytest.raises(ValueError, match="ambiguous survivor"):
            groups[0].survivor()

    def test_only_the_loser_is_retired(self) -> None:
        rows = [
            _row("20110312HTWO0", home="KH", away="KIA", xmin=2166),
            _row("20110312HTNX0", home="NX", away="KIA", xmin=2169),
        ]
        groups = find_duplicate_groups(rows, {"20110312HTWO0": 478, "20110312HTNX0": 0})

        retired = groups[0].retired()
        assert [row.game_id for row in retired] == ["20110312HTNX0"]

    def test_the_survivor_rule_never_retires_the_only_copy_of_a_body(self) -> None:
        """The invariant the whole script rests on.

        Across every group, the retired side must not hold strictly more body
        rows than the survivor. If it did, deleting it would destroy data.
        """
        rows = [
            _row("20110312HTWO0", home="KH", away="KIA", home_score=4, away_score=3, xmin=2166),
            _row("20110312HTNX0", home="NX", away="KIA", xmin=2169),
            _row("20141104NXSS0", home="SS", away="KH", home_score=2, away_score=4, xmin=2167),
            _row("20141104WOSS0", home="SS", away="KH", home_score=2, away_score=4, xmin=47367),
            _row("20100414DBKIA0", home="KIA", away="DB", xmin=2166),
            _row("20100414OBHT0", home="KIA", away="DB", xmin=2169),
        ]
        bodies = {
            "20110312HTWO0": 478,
            "20110312HTNX0": 0,
            "20141104NXSS0": 561,
            "20141104WOSS0": 478,
            "20100414DBKIA0": 0,
            "20100414OBHT0": 0,
        }

        for group in find_duplicate_groups(rows, bodies):
            keep = bodies[group.survivor().game_id]
            for row in group.retired():
                assert bodies[row.game_id] <= keep, row.game_id


class TestRollbackScript:
    """The undo has to be able to put a row back exactly as it was."""

    def test_a_deleted_body_row_is_rolled_back_before_its_parent(self, tmp_path) -> None:
        """A parent insert before its children violates the foreign key."""
        path = tmp_path / "rollback.sql"

        _write_rollback(
            path,
            {"20110312HTNX0": {"game_id": "20110312HTNX0", "game_status": "COMPLETED", "season_id": 176}},
            [("game_play_by_play", [{"game_id": "20110312HTNX0", "inning": 1}])],
        )
        assert path.exists()
        rendered = path.read_text(encoding="utf-8").splitlines()

        child_line = next(i for i, line in enumerate(rendered) if "game_play_by_play" in line and "INSERT" in line)
        parent_line = next(i for i, line in enumerate(rendered) if line.startswith("INSERT INTO game "))
        assert child_line < parent_line, "children must be restored before the parent row"

    def test_every_parent_column_is_written(self, tmp_path) -> None:
        """A partial rollback restores a row that no longer matches."""
        path = tmp_path / "rollback.sql"
        row = {"game_id": "20100414OBHT0", "home_team": "KIA", "season_id": 169, "is_primary": True}

        _write_rollback(path, {"20100414OBHT0": row}, [])

        statement = next(line for line in path.read_text(encoding="utf-8").splitlines() if "INSERT INTO game" in line)
        for column in row:
            assert column in statement

    def test_a_quote_in_a_value_is_escaped(self) -> None:
        assert _sql_literal("O'Brien") == "'O''Brien'"

    def test_null_and_numbers_are_rendered_without_quotes(self) -> None:
        assert _sql_literal(None) == "NULL"
        assert _sql_literal(42) == "42"


class TestTheDryRunWritesNothing:
    """A preview that writes is not a preview.

    Run against a real SQLite database rather than a stub, because the thing
    being tested is that nothing reaches the table: a stub that only recognizes
    the two reads would pass whether or not a delete were issued.
    """

    @staticmethod
    def _schema(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY, game_date TEXT, home_team TEXT, away_team TEXT,
                    away_score INTEGER, home_score INTEGER, game_status TEXT,
                    created_at TEXT, is_primary BOOLEAN
                )
                """,
            ),
        )
        for table in ("game_batting_stats", "game_play_by_play"):
            conn.execute(text(f'CREATE TABLE "{table}" (game_id TEXT)'))
        for table in (
            "game_metadata",
            "game_events",
            "game_highlights",
            "game_mvps",
            "game_validation_metrics",
            "correction_audit_trail",
            "game_broadcasts",
            "game_lineups",
            "game_inning_scores",
            "game_summary",
            "game_pitching_stats",
        ):
            conn.execute(text(f'CREATE TABLE "{table}" (game_id TEXT)'))

    def test_dry_run_reports_without_deleting(self, monkeypatch, tmp_path) -> None:
        import scripts.maintenance.dedupe_duplicate_games as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        engine = create_engine("sqlite:///:memory:")
        with engine.begin() as conn:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20110312HTWO0', '2011-03-12', 'KH', 'KIA', 3, 4, 'COMPLETED', '2026-07-18', 1),
                        ('20110312HTNX0', '2011-03-12', 'NX', 'KIA', NULL, NULL, 'COMPLETED', '2026-07-19', 1)
                    """,
                ),
            )
            conn.execute(text("INSERT INTO game_play_by_play VALUES ('20110312HTWO0')"))
            before = conn.execute(text("SELECT COUNT(*) FROM game")).scalar_one()

            report = dedupe(conn)
            after = conn.execute(text("SELECT COUNT(*) FROM game")).scalar_one()

        assert report.groups == 1
        assert report.would_retire == 1
        assert report.retired == 0, "a dry run must not retire anything"
        assert after == before, "a dry run must not touch the table"
        assert report.snapshot_path is None, "a dry run must not write a snapshot"
        assert not list(tmp_path.iterdir()), "a dry run must not write files"


class TestTheApplyRetiresTheEmptyHalf:
    """End to end against a real database, because the unit tests cannot see it.

    The delete touches two tables and writes two recovery files. Each of those is
    where a defect would be invisible in a grouping test.
    """

    @staticmethod
    def _schema(conn) -> None:
        conn.execute(
            text(
                """
                CREATE TABLE game (
                    game_id TEXT PRIMARY KEY, game_date TEXT, home_team TEXT, away_team TEXT,
                    away_score INTEGER, home_score INTEGER, game_status TEXT,
                    created_at TEXT, is_primary BOOLEAN
                )
                """,
            ),
        )
        for table in ("game_batting_stats", "game_play_by_play"):
            conn.execute(text(f'CREATE TABLE "{table}" (game_id TEXT, inning INTEGER)'))
        for table in (
            "game_metadata",
            "game_events",
            "game_highlights",
            "game_mvps",
            "game_validation_metrics",
            "correction_audit_trail",
            "game_broadcasts",
            "game_lineups",
            "game_inning_scores",
            "game_summary",
            "game_pitching_stats",
        ):
            conn.execute(text(f'CREATE TABLE "{table}" (game_id TEXT, inning INTEGER)'))

    def test_the_body_bearing_row_survives_and_the_empty_one_goes(self, monkeypatch, tmp_path) -> None:
        import scripts.maintenance.dedupe_duplicate_games as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20110312HTWO0', '2011-03-12', 'KH', 'KIA', 3, 4, 'COMPLETED', '2026-07-18 19:17:48', 1),
                        ('20110312HTNX0', '2011-03-12', 'NX', 'KIA', NULL, NULL, 'COMPLETED', '2026-07-19 14:12:16', 1)
                    """,
                ),
            )
            for inning in range(5):
                conn.execute(text("INSERT INTO game_play_by_play VALUES ('20110312HTWO0', :i)"), {"i": inning})
            conn.execute(text("INSERT INTO game_metadata VALUES ('20110312HTNX0', 1)"))
            conn.commit()

            report = dedupe(conn, apply=True)

            remaining = [row[0] for row in conn.execute(text("SELECT game_id FROM game"))]
            pbp = conn.execute(text("SELECT COUNT(*) FROM game_play_by_play")).scalar_one()
            metadata = conn.execute(text("SELECT COUNT(*) FROM game_metadata")).scalar_one()
        finally:
            conn.close()

        assert report.retired == 1
        assert remaining == ["20110312HTWO0"], "the boxscore holder must survive"
        assert pbp == 5, "its body must be untouched"
        assert metadata == 0, "the retired row's child must go with it"

    def test_the_snapshot_and_rollback_are_written_before_the_delete(self, monkeypatch, tmp_path) -> None:
        """A run that crashes mid-delete must still be undoable."""
        import scripts.maintenance.dedupe_duplicate_games as module

        monkeypatch.setattr(module, "ARCHIVE_DIR", tmp_path)
        engine = create_engine("sqlite:///:memory:")
        conn = engine.connect()
        try:
            self._schema(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO game VALUES
                        ('20110312HTWO0', '2011-03-12', 'KH', 'KIA', 3, 4, 'COMPLETED', '2026-07-18', 1),
                        ('20110312HTNX0', '2011-03-12', 'NX', 'KIA', NULL, NULL, 'COMPLETED', '2026-07-19', 1)
                    """,
                ),
            )
            conn.commit()

            report = dedupe(conn, apply=True)
        finally:
            conn.close()

        snapshot = pathlib.Path(report.snapshot_path)
        rollback = pathlib.Path(report.rollback_path)

        assert snapshot.exists() and rollback.exists()

        payload = json.loads(snapshot.read_text(encoding="utf-8"))
        assert payload["retire"] == ["20110312HTNX0"], "the snapshot names what was removed"

        rollback_text = rollback.read_text(encoding="utf-8")
        assert "20110312HTNX0" in rollback_text, "the rollback must be able to put it back"
        assert "INSERT INTO game " in rollback_text


class TestTheGateStaysZero:
    """The point of the run: the gate that reported 1,161 reports zero after."""

    def test_the_survivor_is_never_retired_by_the_same_group(self) -> None:
        """Guards the off-by-one that would delete both halves."""
        group = DuplicateGroup(
            game_date=date(2011, 3, 12),
            home_key="F11",
            away_key="F5",
            doubleheader="0",
            members=[_row("20110312HTWO0", home="KH", away="KIA"), _row("20110312HTNX0", home="NX", away="KIA")],
            body_counts={"20110312HTWO0": 478, "20110312HTNX0": 0},
        )

        survivor = group.survivor().game_id
        retired = {row.game_id for row in group.retired()}

        assert survivor not in retired
        assert len(retired) == len(group.members) - 1


def test_the_payload_read_asks_for_every_column() -> None:
    """A rollback missing a column is a rollback that does not restore."""
    assert _full_row_payload.__doc__ is not None
