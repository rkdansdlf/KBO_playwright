"""Contract tests for the RAG league-type metadata repair.

The game-level generators wrote the regular-season constant into every chunk, so
a postseason chunk could not be told apart from a regular-season one and a filter
on ``league_type_code`` returned the wrong rows. These tests pin the two rules
that put the field back in agreement with the season, and pin that the repair is
a metadata write rather than a re-index.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from scripts.maintenance.backfill_rag_league_type_codes import (
    GAME_SCOPED_SOURCES,
    SEASONLESS_SOURCES,
    backfill_league_type_codes,
    game_id_for,
    plan_codes,
)
from src.models.game import Game
from src.models.rag_chunk import RagChunk
from src.models.season import KboSeason


@pytest.fixture
def session() -> Iterator[Session]:
    """Give each test its own SQLite store so the writes are real."""
    engine = create_engine("sqlite://")
    for table in (RagChunk.__table__, Game.__table__, KboSeason.__table__):
        table.create(engine)
    with sessionmaker(bind=engine)() as session:
        yield session


def _season(session: Session, season_id: int, year: int, code: int) -> KboSeason:
    row = KboSeason(
        season_id=season_id,
        season_year=year,
        league_type_code=code,
        league_type_name=f"code-{code}",
    )
    session.add(row)
    return row


def _game(session: Session, game_id: str, season_id: int | None) -> Game:
    row = Game(game_id=game_id, game_date=date(2026, 10, 1), season_id=season_id, is_primary=True)
    session.add(row)
    return row


def _chunk(
    session: Session,
    source_table: str,
    source_row_id: str,
    *,
    code: int | None = 0,
    game_id: str | None = None,
) -> RagChunk:
    meta = {"game_id": game_id} if game_id is not None else {}
    row = RagChunk(
        source_table=source_table,
        source_row_id=source_row_id,
        content="본문",
        index_status="ACTIVE",
        league_type_code=code,
        meta=meta,
    )
    session.add(row)
    return row


class TestReadingTheGame:
    """A chunk's game is where the generator recorded it, not a parsed key."""

    def test_a_game_row_is_its_own_game(self) -> None:
        assert game_id_for("game", "20261025HTSS0", {}) == "20261025HTSS0"

    def test_another_source_reads_the_game_from_its_metadata(self) -> None:
        assert game_id_for("game_lineups", "20261025HTSS0_away", {"game_id": "20261025HTSS0"}) == "20261025HTSS0"

    def test_a_row_without_metadata_has_no_game(self) -> None:
        # Splitting the key would be a guess at a format this function does not
        # own -- the pbp key carries a content digest after the game id.
        assert game_id_for("game_play_by_play", "20180324HHWO0_content_20f49ee12f83", None) is None


class TestPlanning:
    """Only rows that disagree with their season are planned."""

    def test_a_postseason_row_is_planned_with_its_postseason_code(self) -> None:
        rows = [(1, "game", "20261025HTSS0", 0, {})]

        assert plan_codes(rows, {"20261025HTSS0": 4}) == [(1, 0, 4)]

    def test_a_row_that_already_agrees_is_not_planned(self) -> None:
        rows = [(1, "game", "20261001AAAA0", 0, {})]

        assert plan_codes(rows, {"20261001AAAA0": 0}) == []

    def test_a_game_the_map_does_not_hold_is_left_alone(self) -> None:
        # Writing 0 here would answer "this is the regular season" to a question
        # whose only honest answer is "I do not know", on the field a search
        # filters by.
        rows = [(1, "game", "20261025HTSS0", 0, {})]

        assert plan_codes(rows, {}) == []

    def test_a_row_whose_game_is_missing_is_not_reset_to_the_regular_season(self) -> None:
        # The row already states something specific. A game the map cannot
        # resolve must not be turned into a reason to overwrite it with the
        # regular-season default -- that would destroy a correct code while
        # believing it was filling in a gap.
        rows = [(1, "game", "20261025HTSS0", 4, {})]

        assert plan_codes(rows, {}) == []

    def test_a_seasonless_source_is_cleared_rather_than_set(self) -> None:
        rows = [(1, "teams", "HH", 0, {})]

        assert plan_codes(rows, {}) == [(1, 0, None)]

    def test_the_two_rule_sets_do_not_overlap(self) -> None:
        assert frozenset() == GAME_SCOPED_SOURCES & SEASONLESS_SOURCES


class TestTheBackfillWrites:
    """The repair updates a metadata column and leaves the embedding alone."""

    def test_a_dry_run_reports_without_writing(self, session: Session) -> None:
        _season(session, 313, 2026, 4)
        _game(session, "20261025HTSS0", 313)
        row = _chunk(session, "game", "20261025HTSS0", code=0)
        session.commit()

        report = backfill_league_type_codes(session, apply=False)

        session.refresh(row)
        assert row.league_type_code == 0
        assert report.pending == 1
        assert report.updated == 0

    def test_an_apply_rewrites_the_code(self, session: Session) -> None:
        _season(session, 313, 2026, 4)
        _game(session, "20261025HTSS0", 313)
        row = _chunk(session, "game", "20261025HTSS0", code=0)
        session.commit()

        report = backfill_league_type_codes(session, apply=True)

        session.refresh(row)
        assert row.league_type_code == 4
        assert report.updated == 1

    def test_a_postseason_chunk_beside_a_regular_one_is_told_apart(self, session: Session) -> None:
        _season(session, 2026, 2026, 0)
        _season(session, 313, 2026, 4)
        _game(session, "20261001AAAA0", 2026)
        _game(session, "20261025HTSS0", 313)
        _chunk(session, "game", "20261001AAAA0", code=0)
        _chunk(session, "game", "20261025HTSS0", code=0)
        session.commit()

        backfill_league_type_codes(session, apply=True)

        stored = {
            row[0]: row[1] for row in session.execute(select(RagChunk.source_row_id, RagChunk.league_type_code)).all()
        }
        assert stored == {"20261001AAAA0": 0, "20261025HTSS0": 4}

    def test_a_team_row_is_cleared(self, session: Session) -> None:
        row = _chunk(session, "teams", "HH", code=0)
        session.commit()

        backfill_league_type_codes(session, apply=True)

        session.refresh(row)
        assert row.league_type_code is None

    def test_the_embedding_and_hash_are_not_touched(self, session: Session) -> None:
        # The repair must stay a metadata write: re-embedding would spend the
        # budget for a filter column, and the embeddings already describe their
        # rows because the code is not part of the content hash.
        _season(session, 313, 2026, 4)
        _game(session, "20261025HTSS0", 313)
        row = _chunk(session, "game", "20261025HTSS0", code=0)
        row.embedding = [0.1, 0.2]
        row.content_hash = "abc123"
        distinctly = RagChunk(
            source_table="game_lineups",
            source_row_id="20261025HTSS0_away",
            content="본문",
            index_status="ACTIVE",
            league_type_code=0,
            meta={"game_id": "20261025HTSS0"},
            embedding=[0.3, 0.4],
            content_hash="def456",
        )
        session.add(distinctly)
        session.commit()

        backfill_league_type_codes(session, apply=True)

        session.refresh(row)
        session.refresh(distinctly)
        assert row.embedding == [0.1, 0.2]
        assert row.content_hash == "abc123"
        assert distinctly.embedding == [0.3, 0.4]
        assert distinctly.content_hash == "def456"

    def test_a_backup_is_written_before_the_rows_change(self, session: Session, tmp_path: Path) -> None:
        _season(session, 313, 2026, 4)
        _game(session, "20261025HTSS0", 313)
        _chunk(session, "game", "20261025HTSS0", code=0)
        session.commit()
        target = tmp_path / "codes.csv"

        report = backfill_league_type_codes(session, apply=True, backup_path=target)

        with target.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows == [
            {
                "chunk_id": "1",
                "source_table": "game",
                "source_row_id": "20261025HTSS0",
                "previous_code": "0",
                "desired_code": "4",
            },
        ]
        assert report.backup_path == str(target)

    def test_a_second_run_finds_nothing_left_to_do(self, session: Session) -> None:
        _season(session, 313, 2026, 4)
        _game(session, "20261025HTSS0", 313)
        _chunk(session, "game", "20261025HTSS0", code=0)
        session.commit()

        backfill_league_type_codes(session, apply=True)
        second = backfill_league_type_codes(session, apply=True)

        assert second.pending == 0
        assert second.updated == 0

    def test_an_unknown_game_is_counted_but_not_written(self, session: Session) -> None:
        _chunk(session, "game", "20261025HTSS0", code=0)
        session.commit()

        report = backfill_league_type_codes(session, apply=True)

        assert report.unknown_game == 1
        assert report.updated == 0

    def test_the_store_is_read_through_a_fresh_connection(self, tmp_path: Path) -> None:
        engine = create_engine(f"sqlite:///{tmp_path / 'store.db'}")
        for table in (RagChunk.__table__, Game.__table__, KboSeason.__table__):
            table.create(engine)
        make_session = sessionmaker(bind=engine)
        writer = make_session()
        try:
            _season(writer, 313, 2026, 4)
            _game(writer, "20261025HTSS0", 313)
            _chunk(writer, "game", "20261025HTSS0", code=0)
            writer.commit()

            backfill_league_type_codes(writer, apply=True)

            with make_session() as reader:
                code = reader.execute(
                    select(RagChunk.league_type_code).where(RagChunk.source_table == "game"),
                ).scalar_one()
            assert code == 4
        finally:
            writer.close()
            engine.dispose()
