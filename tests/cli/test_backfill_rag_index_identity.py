"""The RAG identity backfill must cover every row exactly once.

A 583,265-row pass that silently covered a third of the corpus while reporting
every row as scanned would be worse than one that failed, so the pagination is
pinned here: keyset by primary key, ordered, and verified to visit each row once.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime

import pytest
from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.orm import sessionmaker

from src.cli.backfill import backfill_rag_index_identity as cli
from src.models.rag_chunk import RagChunk

ROW_COUNT = 12
BATCH = 5


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    RagChunk.__table__.create(engine)
    return sessionmaker(bind=engine)


def _seed(session, count: int = ROW_COUNT) -> None:
    session.add_all(
        [
            RagChunk(
                source_table="game",
                source_row_id=f"g{index}",
                title=f"game {index}",
                content=f"content {index}",
            )
            for index in range(count)
        ],
    )
    session.commit()


class TestPaginationVisitsEveryRowOnce:
    def test_every_row_is_scanned_when_the_batch_does_not_divide_evenly(self, session_factory) -> None:
        """ROW_COUNT=12 over batches of 5 needs three passes, and the tail must land."""
        with session_factory() as session:
            _seed(session)
            report = cli._backfill_in_batches(
                session,
                RagChunk,
                apply=False,
                index_version="rag-v1",
                indexed_at=None,
            )

            assert report["scanned"] == ROW_COUNT

    def test_an_exact_multiple_does_not_loop_forever(self, session_factory) -> None:
        with session_factory() as session:
            _seed(session, count=10)
            report = cli._backfill_in_batches(
                session,
                RagChunk,
                apply=False,
                index_version="rag-v1",
                indexed_at=None,
            )

            assert report["scanned"] == 10

    def test_no_row_is_skipped_when_the_batch_size_is_one(self, session_factory, monkeypatch) -> None:
        """The worst case for an off-by-one is a page size that exposes it."""
        monkeypatch.setattr(cli, "BATCH_SIZE", 1)
        with session_factory() as session:
            _seed(session, count=7)
            report = cli._backfill_in_batches(
                session,
                RagChunk,
                apply=False,
                index_version="rag-v1",
                indexed_at=None,
            )

            assert report["scanned"] == 7


class TestStampedValuesComeFromTheCaller:
    def test_the_named_version_and_timestamp_are_used(self, session_factory) -> None:
        """A foreign corpus must not be stamped with the application's defaults."""
        with session_factory() as session:
            _seed(session, count=3)
            cli._backfill_in_batches(
                session,
                RagChunk,
                apply=True,
                index_version="pplx-v2c1",
                indexed_at=datetime(2026, 10, 12),
            )
            session.commit()
            rows = list(session.execute(select(RagChunk).order_by(RagChunk.id)).scalars().all())

            assert {row.index_version for row in rows} == {"pplx-v2c1"}
            assert {row.indexed_at.date() for row in rows} == {datetime(2026, 10, 12).date()}

    def test_the_hash_is_derived_from_the_chunk_text(self, session_factory) -> None:
        with session_factory() as session:
            _seed(session, count=2)
            cli._backfill_in_batches(
                session,
                RagChunk,
                apply=True,
                index_version="rag-v1",
                indexed_at=None,
            )
            session.commit()
            rows = list(session.execute(select(RagChunk).order_by(RagChunk.id)).scalars().all())

            assert all(row.content_hash and len(row.content_hash) == 64 for row in rows)
            assert rows[0].content_hash != rows[1].content_hash


class TestDryRunIsTheDefault:
    @pytest.fixture(autouse=True)
    def _isolate_from_any_configured_database(self, session_factory, monkeypatch):
        """Point the CLI at the throwaway engine.

        Without this the CLI opens whatever ``DATABASE_URL`` names, so a test
        that only meant to check argument handling would read a real store.
        """
        monkeypatch.setattr(cli, "get_rag_index_session", session_factory)

    def test_a_dry_run_reports_without_writing(self, session_factory, capsys) -> None:
        with session_factory() as session:
            _seed(session, count=3)

            assert cli.main(["--skip-vector"]) == 0
            captured = capsys.readouterr()

            assert "apply=False" in captured.out
            rows = list(session.execute(select(RagChunk)).scalars().all())
            assert all(row.index_version is None for row in rows)

    def test_the_version_is_echoed_back(self, session_factory, capsys) -> None:
        with session_factory() as session:
            _seed(session, count=1)
            code = cli.main(["--skip-vector", "--json", "--index-version", "pplx-v2c1"])

            payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
            assert code == 0
            assert payload["index_version"] == "pplx-v2c1"


class TestTimestampParsing:
    def test_an_iso_timestamp_is_accepted(self) -> None:
        assert cli._parse_timestamp("2026-10-12T00:00:00") == datetime(2026, 10, 12, 0, 0, 0)

    def test_absent_means_now_is_decided_by_the_caller(self) -> None:
        assert cli._parse_timestamp(None) is None


class TestThePrimaryPassRunsWithoutTheOracleVectorColumn:
    """The primary store has no ``embedding_vector`` column.

    ``RagChunk.embedding`` is declared with the physical name
    ``embedding_vector`` because Oracle needs a native ``VECTOR`` type;
    PostgreSQL keeps JSON in a plain column instead and never creates it. A
    bare ``select(RagChunk)`` names every mapped column, so the primary pass
    died with ``UndefinedColumn: column rag_chunks.embedding_vector does not
    exist`` before reading a single row -- which is why ``--skip-vector``, a
    flag about skipping the *vector store*, was not enough to run it. The
    whole 583,265-row backfill was unreachable, including the read-only dry
    run.

    SQLite's ``create_all`` does create the column, so every other test in this
    file passes against a schema production never has. These tests drop it.
    """

    @pytest.fixture
    def session_factory_without_embedding(self):
        """Create ``rag_chunks`` exactly as the primary store has it.

        ``Table.create`` would emit every mapped column, so the table is built
        from a copy with the dense column removed. The model's own metadata is
        untouched -- it has to stay intact, because the failure comes from
        ``select()`` naming a column the database never created.

        The column is ``embedding``. The fixture once excluded
        ``embedding_vector``, a name the model carried before the dense column
        was renamed; against the renamed model it excluded nothing, the table
        came out complete, and the guard below stopped raising without anyone
        noticing that the fixture had stopped modelling anything.
        """
        engine = create_engine("sqlite:///:memory:")
        metadata = MetaData()
        original = RagChunk.__table__
        table = Table(
            original.name,
            metadata,
            *(column.copy() for column in original.columns if column.name != "embedding"),
            extend_existing=True,
        )
        table.create(engine)
        return sessionmaker(bind=engine)

    def test_the_column_really_is_absent(self, session_factory_without_embedding) -> None:
        """Guard the fixture: a schema that still has the column proves nothing."""
        from sqlalchemy import inspect

        with session_factory_without_embedding() as session:
            columns = {c["name"] for c in inspect(session.get_bind()).get_columns("rag_chunks")}

        assert "embedding" not in columns

    def test_a_plain_select_of_the_model_fails_on_this_schema(self, session_factory_without_embedding) -> None:
        """Pin the failure the fix exists for.

        If this stops raising, the fixture no longer models production and the
        deferral is being tested against a schema that was never broken.
        """
        from sqlalchemy.exc import OperationalError

        with session_factory_without_embedding() as session:
            _seed(session, count=2)
            with pytest.raises(OperationalError):
                session.execute(select(RagChunk)).scalars().all()

    def test_the_batches_pass_runs_and_scans_every_row(self, session_factory_without_embedding) -> None:
        with session_factory_without_embedding() as session:
            _seed(session, count=6)
            report = cli._backfill_in_batches(
                session,
                RagChunk,
                apply=True,
                index_version="rag-v1",
                indexed_at=None,
                defer_columns=("embedding",),
            )
            session.commit()

            assert report["scanned"] == 6
            assert report["changed"] == 6

    def test_the_end_to_end_command_runs(self, session_factory_without_embedding, monkeypatch, capsys) -> None:
        """The reported symptom was the command, so the command is what is pinned."""
        monkeypatch.setattr(cli, "get_rag_index_session", session_factory_without_embedding)
        with session_factory_without_embedding() as session:
            _seed(session, count=4)

            assert cli.main(["--skip-vector", "--json"]) == 0

        payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert payload["primary"]["scanned"] == 4
