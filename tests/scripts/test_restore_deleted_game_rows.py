"""Restore of `game` rows deleted while their metadata survived.

The deletion this script undoes had a specific shape: seven rows, spanning two
whole dates, with every one of their ``game_metadata`` rows still present. An
``ON DELETE CASCADE`` parent delete takes children with it, so the surviving
metadata is what rules out a cascade and points at a delete that targeted
``game`` alone.

The tests therefore pin the three properties that make the restore safe rather
than merely possible: it only fills genuine gaps, it refuses to re-derive a
status the caller did not name, and re-running it writes nothing.
"""

from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.restore_deleted_game_rows import (
    ENV_GATE,
    _planned_row,
    main,
    restore_year,
)
from src.models.game import Game, GameMetadata
from src.models.team_history import TeamHistory

ANSWER_SHAPE = {
    "game_id": "0",
    "game_date": "1995-06-20",
    "stadium": "잠실",
    "home_team": "OB",
    "away_team": "LT",
    "home_score": 9,
    "away_score": 4,
}

MISSING_IDS = (
    "19950620LTOB0",
    "19950621LTOB0",
    "19950620LGTP0",
    "19950620HTSL0",
    "19950621HTSL0",
    "19950620SSHH0",
    "19950621SSHH0",
)


@pytest.fixture
def answer_dir(tmp_path, monkeypatch):
    """Write a small 1995 answer set and point the script at it."""
    directory = tmp_path / "archives"
    directory.mkdir()
    games = []
    for index, game_id in enumerate(MISSING_IDS):
        row = dict(ANSWER_SHAPE)
        row["game_id"] = f"{index}{game_id[-1]}"
        row["game_date"] = "1995-06-20" if game_id.startswith("19950620") else "1995-06-21"
        row["away_team"] = game_id[8:10]
        row["home_team"] = game_id[10:12]
        games.append(row)
    # One row that is already present, so the "fill only gaps" property is testable.
    kept = dict(ANSWER_SHAPE)
    kept["game_id"] = "0"
    kept["game_date"] = "1995-06-25"
    kept["away_team"] = "OB"
    kept["home_team"] = "HT"
    games.append(kept)
    (directory / "1995_answer_set_final.json").write_text(json.dumps(games), encoding="utf-8")

    import scripts.historical.apply_season_to_db as apply_module

    monkeypatch.setattr(apply_module, "ANSWER_DIR", directory)
    monkeypatch.setattr("scripts.maintenance.restore_deleted_game_rows.load_answer_set", apply_module.load_answer_set)
    return directory


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (Game.__table__, GameMetadata.__table__, TeamHistory.__table__):
        table.create(engine)
    return sessionmaker(bind=engine)


def _seed_present(session, game_id: str = "19950625OBHT0") -> None:
    session.add(Game(game_id=game_id, game_date=date(1995, 6, 25), game_status="CANCELLED", is_primary=True))
    session.add(GameMetadata(game_id=game_id, stadium_code="JAMSIL"))
    session.commit()


class TestDryRunFillsOnlyGaps:
    def test_it_reports_the_missing_rows_and_writes_nothing(self, session_factory, answer_dir) -> None:
        with session_factory() as session:
            _seed_present(session)
            report = restore_year(session, 1995)

            assert report.applied is False
            assert report.restored == 0
            assert report.missing_in_db == len(MISSING_IDS)
            assert report.already_present == 1
            assert sorted(row.game_id for row in report.rows) == sorted(MISSING_IDS)
            assert session.query(Game).count() == 1

    def test_a_date_filter_narrows_the_report(self, session_factory, answer_dir) -> None:
        with session_factory() as session:
            _seed_present(session)
            report = restore_year(session, 1995, only_dates=[date(1995, 6, 20)])

            assert sorted(row.game_id for row in report.rows) == sorted(
                i for i in MISSING_IDS if i.startswith("19950620")
            )


class TestTheRestoreIsIdempotent:
    def test_re_running_writes_nothing(self, session_factory, answer_dir) -> None:
        with session_factory() as session:
            _seed_present(session)
            restore_year(session, 1995, apply=True)
            first_count = session.query(Game).count()

            report = restore_year(session, 1995, apply=True)

            assert first_count == 1 + len(MISSING_IDS)
            assert report.missing_in_db == 0
            assert report.restored == 0
            assert session.query(Game).count() == first_count


class TestTheRestoreDoesNotReDeriveTheStatus:
    def test_the_restored_status_is_the_named_one(self, session_factory, answer_dir) -> None:
        with session_factory() as session:
            _seed_present(session)
            restore_year(session, 1995, apply=True, status="CANCELLED")

            statuses = {row.game_id: row.game_status for row in session.query(Game).all()}

            assert all(status == "CANCELLED" for status in statuses.values())

    def test_completed_is_never_the_default(self) -> None:
        """Restoring as COMPLETED would re-run the 2026-09-23 transition."""
        import inspect

        from scripts.maintenance.restore_deleted_game_rows import restore_year as restore

        default = inspect.signature(restore).parameters["status"].default

        assert default == "CANCELLED"


class TestMetadataIsReportedNotInvented:
    def test_a_row_without_metadata_is_reported(self, session_factory, answer_dir) -> None:
        """The metadata survived the deletion.

        Its absence would mean the restore is looking at a different loss than
        the one it was written for, so the rows are reported rather than
        invented.

        """
        with session_factory() as session:
            _seed_present(session)
            report = restore_year(session, 1995)

            assert sorted(report.metadata_missing) == sorted(MISSING_IDS)


class TestTheWriteGate:
    def test_apply_without_the_environment_gate_exits_three(self, session_factory, answer_dir, monkeypatch) -> None:
        monkeypatch.delenv(ENV_GATE, raising=False)
        monkeypatch.setattr(
            "scripts.maintenance.restore_deleted_game_rows._open_session", lambda _url: session_factory()
        )

        assert main(["--year", "1995", "--apply"]) == 3
        with session_factory() as session:
            assert session.query(Game).count() == 0

    def test_apply_with_the_gate_writes(self, session_factory, answer_dir, monkeypatch) -> None:
        monkeypatch.setenv(ENV_GATE, "1")
        monkeypatch.setattr(
            "scripts.maintenance.restore_deleted_game_rows._open_session", lambda _url: session_factory()
        )

        assert main(["--year", "1995", "--apply"]) == 0
        with session_factory() as session:
            present = {row.game_id for row in session.query(Game).all()}
            assert set(MISSING_IDS) <= present

    def test_dry_run_is_the_default(self, session_factory, answer_dir, monkeypatch) -> None:
        monkeypatch.setenv(ENV_GATE, "1")
        monkeypatch.setattr(
            "scripts.maintenance.restore_deleted_game_rows._open_session", lambda _url: session_factory()
        )

        assert main(["--year", "1995"]) == 0
        with session_factory() as session:
            assert session.query(Game).count() == 0


class TestUnsupportedInput:
    def test_a_year_without_an_answer_set_is_refused(self, session_factory, monkeypatch) -> None:
        monkeypatch.setattr(
            "scripts.maintenance.restore_deleted_game_rows._open_session", lambda _url: session_factory()
        )

        assert main(["--year", "2026", "--apply"]) == 2

    def test_nothing_to_restore_without_a_target(self, session_factory, monkeypatch) -> None:
        monkeypatch.setattr(
            "scripts.maintenance.restore_deleted_game_rows._open_session", lambda _url: session_factory()
        )

        assert main([]) == 2


class TestInsertShape:
    def test_the_row_carries_the_answer_set_fields_and_no_guessed_franchise(self) -> None:
        from scripts.maintenance.restore_deleted_game_rows import RestoreRow

        planned = _planned_row(
            RestoreRow(
                game_id="19950620LTOB0",
                game_date=date(1995, 6, 20),
                away_team="LT",
                home_team="OB",
                away_score=4,
                home_score=9,
                stadium="잠실",
                game_status="CANCELLED",
            ),
        )

        assert planned["game_id"] == "19950620LTOB0"
        assert planned["away_score"] == 4
        assert planned["home_score"] == 9
        assert planned["season_id"] == 199500
        assert planned["is_primary"] is True
        # The answer set carries no franchise id, and a guessed one is worse
        # than a null the dedup pass can still resolve.
        assert "away_franchise_id" not in planned
        assert "home_franchise_id" not in planned
