"""Contract tests for the RAG chunk purge that only touches unregenerable rows.

The store carries rows three different generators wrote under three different key
conventions, and the audit reports every one of them as an ``embedding_missing``
finding. A finding is not the same thing as a gap: a row the current generator
can no longer produce has no source to be re-embedded from, so re-embedding it
would preserve a key nothing else refers to. These tests pin the rules that
separate the two, and pin that the lifecycle path the write takes is one
``can_transition`` actually allows.
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from scripts.maintenance import purge_orphan_rag_chunks as purge_module
from scripts.maintenance.purge_orphan_rag_chunks import (
    INDEXED_SOURCE_TABLES,
    absent_source_identity,
    classify_unregenerable,
    deletion_path,
    purge_unregenerable,
    restore_from_backup,
)
from src.cli.rag import build_rag_index
from src.models.game import Game, GameLineup
from src.models.rag_chunk import RagChunk
from src.services.rag_index_lifecycle import can_transition


@pytest.fixture
def session() -> Iterator[Session]:
    """Give each test its own SQLite store for the lifecycle writes to be real."""
    engine = create_engine("sqlite://")
    RagChunk.__table__.create(engine)
    Game.__table__.create(engine)
    GameLineup.__table__.create(engine)
    with sessionmaker(bind=engine)() as session:
        yield session


def _game(session: Session, game_id: str) -> Game:
    """Put the parent game row in place, with no lineup rows under it."""
    row = Game(game_id=game_id, game_date=date(2018, 3, 16), is_primary=True)
    session.add(row)
    return row


def _lineup_source(session: Session, game_id: str, *, side: str = "away") -> GameLineup:
    """Put one real lineup row in the source table the rule reads."""
    row = GameLineup(
        game_id=game_id,
        team_side=side,
        player_name="홍길동",
        appearance_seq=1,
        batting_order=1,
    )
    session.add(row)
    return row


def _chunk(
    session: Session,
    source_table: str,
    source_row_id: str,
    *,
    status: str = "ACTIVE",
    embedding: list[float] | None = None,
) -> RagChunk:
    row = RagChunk(
        source_table=source_table,
        source_row_id=source_row_id,
        content="본문",
        index_status=status,
        meta={},
    )
    if embedding is not None:
        # Passing embedding=None explicitly is *not* the state the audit reports:
        # the SQLite JSON variant binds it as the text 'null', while an absent
        # value leaves the column SQL NULL. The store's missing-embedding rows are
        # the latter, so the helper has to reproduce that rather than Python's None.
        row.embedding = embedding
    session.add(row)
    return row


class TestWhatCountsAsUnregenerable:
    """A row is unregenerable only when a rule names why, never by default."""

    def test_a_table_no_source_builds_is_unregenerable(self) -> None:
        assert classify_unregenerable("game_metadata", "20261025HTSS0") == "source_table_not_indexed"

    def test_an_absolute_source_path_cannot_be_reproduced_from_the_local_root(self) -> None:
        reason = classify_unregenerable("markdown_docs", "/app/docs/kbo_knowledge/2025_fa_market.md")
        assert reason == "absolute_source_path"

    def test_the_legacy_numeric_key_convention_is_unregenerable(self) -> None:
        assert classify_unregenerable("player_season_batting", "id=1096") == "legacy_key_convention"

    def test_a_partitioned_legacy_key_is_not_named_yet(self) -> None:
        # ``id=1873#part1`` is the same legacy convention with a chunk suffix and
        # accounts for 5,564 of the 5,589 rows the audit reports as
        # ``REINDEX_REQUIRED``. It is deliberately not named: those rows carry
        # embeddings, so the scan never reaches them, and Phase B replaces them
        # under the current key rather than re-embedding a key that is about to
        # disappear. Widening the pattern without also deciding the Phase B order
        # would spend embedding budget on rows that are already scheduled to go.
        assert classify_unregenerable("player_season_batting", "id=1873#part1") is None

    def test_a_current_key_convention_is_left_alone(self) -> None:
        assert classify_unregenerable("player_season_batting", "96616_2001_KIA_REGULAR") is None

    def test_a_lineup_key_is_not_named_by_the_shape_rules_alone(self) -> None:
        # 523 lineup rows have no current counterpart, but their key shape is the
        # one the generator still produces. Shape is not proof of an orphan, so
        # the pure rules stay silent and the reason has to come from measured
        # source absence (see TestSourceAbsence) rather than from a key pattern.
        assert classify_unregenerable("game_lineups", "20180316KTHH0_away") is None

    def test_only_tables_the_build_targets_count_as_indexed(self) -> None:
        assert "game_lineups" in INDEXED_SOURCE_TABLES
        assert "player_season_batting" in INDEXED_SOURCE_TABLES
        assert "game_metadata" not in INDEXED_SOURCE_TABLES

    def test_the_scope_is_read_from_the_build_rather_than_copied(self) -> None:
        # A copied list is the one direction that turns a live gap into a silent
        # delete: a source added to the build would stay "not indexed" here. The
        # equality is the contract; asserting individual names is not enough.
        assert frozenset(build_rag_index._SOURCE_TABLE_BY_SOURCE.values()) == INDEXED_SOURCE_TABLES


class TestSourceAbsence:
    """A key with no source row cannot be re-embedded, but absence is measured."""

    def test_a_lineup_whose_game_has_no_lineup_rows_is_absent(self, session: Session) -> None:
        _lineup_source(session, "20261017LGHT0")
        session.commit()

        absent = absent_source_identity(session, "game_lineups", ["20180316KTHH0_away"])

        assert absent == frozenset({"20180316KTHH0_away"})

    def test_a_game_row_alone_keeps_the_chunk_off_the_list(self, session: Session) -> None:
        # The game exists, so its lineup can still be crawled and the generator
        # would emit this exact key. Retiring it now would make that future row
        # unreachable: DELETED is in TERMINAL_INDEX_STATUSES, so the incremental
        # selector blocks the key rather than re-embedding it, and the reactivation
        # path is only reached from an upsert the selector never produces.
        _game(session, "20180316KTHH0")
        session.commit()

        absent = absent_source_identity(session, "game_lineups", ["20180316KTHH0_away"])

        assert absent == frozenset()

    def test_the_same_key_is_absent_once_the_game_row_is_gone_too(self, session: Session) -> None:
        # No lineup and no game: nothing can emit this key again, so the delete
        # is not a bet on whether a crawler will revisit the season.
        _game(session, "20261017LGHT0")
        session.commit()

        absent = absent_source_identity(session, "game_lineups", ["20180316KTHH0_away"])

        assert absent == frozenset({"20180316KTHH0_away"})

    def test_a_lineup_whose_game_has_rows_is_left_alone(self, session: Session) -> None:
        _lineup_source(session, "20261017LGHT0", side="away")
        session.commit()

        absent = absent_source_identity(session, "game_lineups", ["20261017LGHT0_away"])

        assert absent == frozenset()

    def test_absence_is_decided_per_game_not_per_side(self, session: Session) -> None:
        # Only the away side is stored for this game. The home chunk still has a
        # source game with lineups, so it is not evidence of absence -- the
        # generator may simply have produced one side. Deciding per side would
        # retire a row whose game is present.
        _lineup_source(session, "20261017LGHT0", side="away")
        session.commit()

        absent = absent_source_identity(session, "game_lineups", ["20261017LGHT0_home"])

        assert absent == frozenset()

    def test_an_empty_source_table_is_not_evidence_of_absence(self, session: Session) -> None:
        # A reload empties the table before filling it. Reading that as "no game
        # has lineups" would retire every lineup chunk in the store in one run,
        # and the report would describe it as housekeeping.
        session.commit()

        absent = absent_source_identity(
            session,
            "game_lineups",
            ["20180316KTHH0_away", "20180316KTHH0_home", "20261017LGHT0_away"],
        )

        assert absent == frozenset()

    def test_a_table_with_no_key_parser_is_never_judged_absent(self, session: Session) -> None:
        # The parser is what turns a chunk key back into a source identity. A
        # table without one would need a guess, and a guess here writes deletes.
        _lineup_source(session, "20261017LGHT0")
        session.commit()

        assert absent_source_identity(session, "player_season_batting", ["96616_2001_KIA_REGULAR"]) == frozenset()

    def test_an_unparseable_key_is_never_judged_absent(self, session: Session) -> None:
        _lineup_source(session, "20261017LGHT0")
        session.commit()

        assert absent_source_identity(session, "game_lineups", ["nounderscore"]) == frozenset()


class TestTheLifecyclePathIsOneTheContractAllows:
    """A delete is reached through transitions, not by assigning DELETED."""

    @pytest.mark.parametrize("status", ["ACTIVE", "REINDEX_REQUIRED", "STALE", "PENDING", "INDEXED"])
    def test_every_live_status_reaches_deleted(self, status: str) -> None:
        path = deletion_path(status)
        assert path is not None
        current = status
        for target in path:
            assert can_transition(current, target), (current, target)
            current = target
        assert current == "DELETED"

    def test_active_takes_the_direct_two_step_path(self) -> None:
        assert deletion_path("ACTIVE") == ["DELETE_PENDING", "DELETED"]

    def test_indexed_has_to_pass_through_stale(self) -> None:
        assert deletion_path("INDEXED") == ["STALE", "DELETE_PENDING", "DELETED"]

    @pytest.mark.parametrize("status", ["DELETED", "TOMBSTONED", "PURGED"])
    def test_terminal_rows_have_nothing_left_to_do(self, status: str) -> None:
        assert deletion_path(status) == []


class TestPurgeWrites:
    def test_a_dry_run_marks_nothing(self, session: Session) -> None:
        row = _chunk(session, "game_metadata", "20261025HTSS0")
        session.commit()

        report = purge_unregenerable(session, apply=False)

        session.refresh(row)
        assert row.index_status == "ACTIVE"
        assert report.orphan_count == 1
        assert report.marked == 0

    def test_an_apply_retires_the_row_through_the_contract(self, session: Session) -> None:
        row = _chunk(session, "game_metadata", "20261025HTSS0")
        session.commit()

        report = purge_unregenerable(session, apply=True)

        session.refresh(row)
        assert row.index_status == "DELETED"
        assert report.marked == 1

    def test_a_regenerable_row_keeps_its_embedding_gap(self, session: Session) -> None:
        row = _chunk(session, "game_lineups", "20261017LGHT0_away")
        session.commit()

        report = purge_unregenerable(session, apply=True)

        session.refresh(row)
        assert row.index_status == "ACTIVE"
        assert report.orphan_count == 0

    def test_a_healthy_row_is_not_touched(self, session: Session) -> None:
        row = _chunk(session, "game_metadata", "20261025HTSS0", embedding=[0.1, 0.2])
        session.commit()

        report = purge_unregenerable(session, apply=True)

        session.refresh(row)
        assert row.index_status == "ACTIVE"
        assert report.orphan_count == 0

    def test_a_large_out_of_scope_table_with_embeddings_is_not_a_scope_of_deletion(self, session: Session) -> None:
        # ``game_summary`` holds 68,829 rows whose keys all match the legacy
        # ``id=<n>`` shape and whose table no current source builds -- both rules
        # fire on it. It is *also* fully embedded and served. "No current
        # generator can reproduce it" and "it is not being served" are different
        # claims, and only the second one justifies a delete. The guard is the
        # scan predicate, so this pins the predicate rather than the rules.
        rows = [_chunk(session, "game_summary", f"id={n}", embedding=[0.1, 0.2]) for n in range(50)]
        session.commit()

        report = purge_unregenerable(session, apply=True)

        for row in rows:
            session.refresh(row)
            assert row.index_status == "ACTIVE"
        assert report.orphan_count == 0
        assert report.scanned == 0

    def test_a_lineup_whose_game_has_rows_is_left_alone(self, session: Session) -> None:
        _lineup_source(session, "20261017LGHT0", side="away")
        session.commit()
        _chunk(session, "game_lineups", "20261017LGHT0_away")
        session.commit()

        report = purge_unregenerable(session, apply=True)

        assert report.orphan_count == 0
        assert report.scanned == 1

    def test_a_lineup_with_no_source_game_is_retired(self, session: Session) -> None:
        _lineup_source(session, "20261017LGHT0")
        _chunk(session, "game_lineups", "20180316KTHH0_away")
        session.commit()

        report = purge_unregenerable(session, apply=True, backup_path=None)

        assert report.orphan_count == 1
        assert report.by_reason == {"source_rows_absent": 1}
        with sessionmaker(bind=session.get_bind())() as reader:
            status = reader.execute(
                select(RagChunk.index_status).where(RagChunk.source_table == "game_lineups"),
            ).scalar_one()
        assert status == "DELETED"

    def test_the_report_counts_each_reason_and_table(self, session: Session) -> None:
        _chunk(session, "game_metadata", "20261025HTSS0")
        _chunk(session, "game_metadata", "20261026HTSS0")
        _chunk(session, "player_season_batting", "id=1096")
        _chunk(session, "markdown_docs", "/app/docs/x.md")
        session.commit()

        report = purge_unregenerable(session, apply=False)

        assert report.by_reason == {
            "source_table_not_indexed": 2,
            "legacy_key_convention": 1,
            "absolute_source_path": 1,
        }
        assert report.by_table == {
            "game_metadata": 2,
            "player_season_batting": 1,
            "markdown_docs": 1,
        }

    def test_a_backup_is_written_before_the_rows_change(self, session: Session, tmp_path: Path) -> None:
        _chunk(session, "game_metadata", "20261025HTSS0")
        session.commit()
        target = tmp_path / "orphans.csv"
        observed: list[str] = []
        real_write = purge_module._write_backup

        def spy(path: Path, rows: list[dict[str, str]]) -> str:
            observed.append(session.execute(select(RagChunk.index_status)).scalar_one())
            return real_write(path, rows)

        with patch.object(purge_module, "_write_backup", spy):
            report = purge_unregenerable(session, apply=True, backup_path=target)

        # A backup written after the fact would still pass a content check, so the
        # state is read from the row at the moment the backup is opened.
        assert observed == ["ACTIVE"]
        with target.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows == [
            {
                "source_table": "game_metadata",
                "source_row_id": "20261025HTSS0",
                "reason": "source_table_not_indexed",
                "previous_status": "ACTIVE",
            },
        ]
        assert report.backup_path == str(target)

    def test_a_dry_run_writes_no_backup(self, session: Session, tmp_path: Path) -> None:
        _chunk(session, "game_metadata", "20261025HTSS0")
        session.commit()
        target = tmp_path / "orphans.csv"

        purge_unregenerable(session, apply=False, backup_path=target)

        assert not target.exists()

    def test_a_terminal_row_is_not_counted_as_a_pending_delete(self, session: Session) -> None:
        _chunk(session, "game_metadata", "20261025HTSS0", status="DELETED")
        session.commit()

        report = purge_unregenerable(session, apply=True)

        assert report.orphan_count == 0
        assert report.already_terminal == 1

    def test_the_store_is_read_through_a_fresh_connection(self, tmp_path: Path) -> None:
        # The store has to carry the write for a reader that is not this session.
        # An in-memory store hands every session the same connection, so an
        # uncommitted change would look committed; a file store does not.
        engine = create_engine(f"sqlite:///{tmp_path / 'store.db'}")
        RagChunk.__table__.create(engine)
        make_session = sessionmaker(bind=engine)
        writer = make_session()
        try:
            _chunk(writer, "game_metadata", "20261025HTSS0")
            writer.commit()

            purge_unregenerable(writer, apply=True)

            # Read while the writer is still open: only a commit makes the new
            # status visible on another connection.
            with make_session() as reader:
                status = reader.execute(
                    select(RagChunk.index_status).where(RagChunk.source_table == "game_metadata"),
                ).scalar_one()
            assert status == "DELETED"
        finally:
            writer.close()
            engine.dispose()


class TestRestoreFromBackup:
    """A purge is reversible from its own backup, and only from it."""

    def _backup(self, path: Path, *rows: tuple[str, str, str, str]) -> Path:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["source_table", "source_row_id", "reason", "previous_status"],
            )
            writer.writeheader()
            for source_table, source_row_id, reason, previous_status in rows:
                writer.writerow(
                    {
                        "source_table": source_table,
                        "source_row_id": source_row_id,
                        "reason": reason,
                        "previous_status": previous_status,
                    },
                )
        return path

    def test_a_deleted_row_returns_to_the_status_the_backup_recorded(self, session: Session, tmp_path: Path) -> None:
        row = _chunk(session, "game_metadata", "20261025HTSS0", status="DELETED")
        session.commit()
        backup = self._backup(
            tmp_path / "orphans.csv",
            ("game_metadata", "20261025HTSS0", "source_table_not_indexed", "STALE"),
        )

        report = restore_from_backup(session, backup_path=backup, apply=True)

        session.refresh(row)
        assert row.index_status == "STALE"
        assert report.restored == 1

    def test_a_row_that_is_not_deleted_is_left_where_it_is(self, session: Session, tmp_path: Path) -> None:
        # The store may have moved on since the backup was written. Restoring a
        # row that is serving would be a second unrequested change.
        row = _chunk(session, "game_metadata", "20261025HTSS0", status="ACTIVE")
        session.commit()
        backup = self._backup(
            tmp_path / "orphans.csv",
            ("game_metadata", "20261025HTSS0", "source_table_not_indexed", "ACTIVE"),
        )

        report = restore_from_backup(session, backup_path=backup, apply=True)

        session.refresh(row)
        assert row.index_status == "ACTIVE"
        assert report.restored == 0
        assert report.not_deleted == 1

    def test_a_purged_row_is_not_restored(self, session: Session, tmp_path: Path) -> None:
        # PURGED is the one state the contract gives no way out of, and it means
        # the row's storage is gone. A backup file does not bring it back.
        row = _chunk(session, "game_metadata", "20261025HTSS0", status="PURGED")
        session.commit()
        backup = self._backup(
            tmp_path / "orphans.csv",
            ("game_metadata", "20261025HTSS0", "source_table_not_indexed", "ACTIVE"),
        )

        report = restore_from_backup(session, backup_path=backup, apply=True)

        session.refresh(row)
        assert row.index_status == "PURGED"
        assert report.restored == 0
        assert report.purged == 1

    def test_a_backed_up_key_that_is_gone_from_the_store_is_reported(self, session: Session, tmp_path: Path) -> None:
        session.commit()
        backup = self._backup(
            tmp_path / "orphans.csv",
            ("game_metadata", "20261025HTSS0", "source_table_not_indexed", "ACTIVE"),
        )

        report = restore_from_backup(session, backup_path=backup, apply=True)

        assert report.restored == 0
        assert report.missing == 1

    def test_a_dry_run_reports_without_writing(self, session: Session, tmp_path: Path) -> None:
        row = _chunk(session, "game_metadata", "20261025HTSS0", status="DELETED")
        session.commit()
        backup = self._backup(
            tmp_path / "orphans.csv",
            ("game_metadata", "20261025HTSS0", "source_table_not_indexed", "ACTIVE"),
        )

        report = restore_from_backup(session, backup_path=backup, apply=False)

        session.refresh(row)
        assert row.index_status == "DELETED"
        assert report.restored == 1
        assert report.written is False

    def test_the_restore_leaves_its_own_backup(self, session: Session, tmp_path: Path) -> None:
        _chunk(session, "game_metadata", "20261025HTSS0", status="DELETED")
        session.commit()
        backup = self._backup(
            tmp_path / "orphans.csv",
            ("game_metadata", "20261025HTSS0", "source_table_not_indexed", "ACTIVE"),
        )
        restore_backup = tmp_path / "restore.csv"

        report = restore_from_backup(
            session,
            backup_path=backup,
            apply=True,
            backup_out=restore_backup,
        )

        assert report.backup_path == str(restore_backup)
        with restore_backup.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows == [
            {
                "source_table": "game_metadata",
                "source_row_id": "20261025HTSS0",
                "previous_status": "DELETED",
                "restored_status": "ACTIVE",
            },
        ]

    def test_an_unreadable_previous_status_is_left_alone(self, session: Session, tmp_path: Path) -> None:
        # The backup is a file on disk, so its contents are input, not truth. A
        # status the lifecycle does not define would write a value the rest of the
        # system cannot interpret -- the one write here that bypasses the
        # transition contract still has to land on a state the contract names.
        row = _chunk(session, "game_metadata", "20261025HTSS0", status="DELETED")
        session.commit()
        backup = self._backup(
            tmp_path / "orphans.csv",
            ("game_metadata", "20261025HTSS0", "source_table_not_indexed", "BOGUS"),
        )

        report = restore_from_backup(session, backup_path=backup, apply=True)

        session.refresh(row)
        assert row.index_status == "DELETED"
        assert report.requested == 1
        assert report.restored == 0
        assert report.not_deleted == 0
        assert report.written is False
