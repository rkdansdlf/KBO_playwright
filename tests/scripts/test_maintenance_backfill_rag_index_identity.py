"""Contract tests for the RAG identity backfill left out of migration 051.

``051_rag_index_consistency.sql`` added ``content_hash`` and ``index_version``
as nullable columns and only backfilled ``index_status``. Every row that was
not touched by a later UPSERT kept NULLs, and the audit gate reports those NULLs
as findings. The values are deterministic (``chunk_content_hash`` and
``current_index_version`` are what the UPSERT path itself writes), so filling
them is a recovery of missing values, not a guess.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from scripts.maintenance.backfill_rag_index_identity import (
    BackfillReport,
    backfill_rag_index_identity,
)
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import chunk_content_hash, current_index_version


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    RagChunk.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    active = factory()
    try:
        yield active
    finally:
        active.close()
        engine.dispose()


def _add_chunk(
    session: Session,
    *,
    row_id: str,
    title: str | None = "title",
    content: str = "body",
    content_hash: str | None = None,
    index_version: str | None = None,
) -> None:
    session.add(
        RagChunk(
            source_table="games",
            source_row_id=row_id,
            title=title,
            content=content,
            content_hash=content_hash,
            index_version=index_version,
            index_status="ACTIVE",
            meta={},
        ),
    )
    session.commit()


def _stored(session: Session, row_id: str) -> RagChunk:
    return session.scalars(select(RagChunk).where(RagChunk.source_row_id == row_id)).one()


def test_dry_run_reports_targets_without_writing(session: Session) -> None:
    """A dry run counts the missing values and leaves every row untouched."""
    _add_chunk(session, row_id="a")
    _add_chunk(
        session,
        row_id="b",
        index_version="rag-v1",
        content_hash=chunk_content_hash("title", "body"),
    )

    report = backfill_rag_index_identity(session, apply=False)

    assert report == BackfillReport(
        content_hash_targets=1,
        index_version_targets=1,
        content_hash_written=0,
        index_version_written=0,
        contract_sample=1,
        contract_violations=0,
    )
    assert _stored(session, "a").content_hash is None
    assert _stored(session, "a").index_version is None


def test_apply_fills_the_missing_values_with_the_deterministic_contract(session: Session) -> None:
    """The values written are exactly what the UPSERT path would have written."""
    _add_chunk(session, row_id="a", title="  Alpha  ", content="one   two\nthree")

    report = backfill_rag_index_identity(session, apply=True)
    session.commit()

    stored = _stored(session, "a")
    assert report.content_hash_written == 1
    assert report.index_version_written == 1
    assert stored.content_hash == chunk_content_hash("  Alpha  ", "one   two\nthree")
    assert stored.index_version == current_index_version()


def test_existing_values_are_never_overwritten(session: Session) -> None:
    """A stored value is evidence; the backfill only fills holes."""
    _add_chunk(session, row_id="a", content_hash="b" * 64, index_version="rag-v0")

    report = backfill_rag_index_identity(session, apply=True)
    session.commit()

    stored = _stored(session, "a")
    assert report.content_hash_written == 0
    assert report.index_version_written == 0
    assert stored.content_hash == "b" * 64
    assert stored.index_version == "rag-v0"


def test_every_row_is_reached_across_multiple_batches(session: Session) -> None:
    """Keyset pagination must not stop after the first batch."""
    for index in range(5):
        _add_chunk(session, row_id=f"row-{index}")

    report = backfill_rag_index_identity(session, apply=True, batch_size=2)
    session.commit()

    assert report.content_hash_written == 5
    assert report.index_version_written == 5
    assert all(_stored(session, f"row-{index}").content_hash for index in range(5))


def test_the_contract_sample_reports_stored_hashes_that_disagree(session: Session) -> None:
    """A stored hash that disagrees with the canonical function is reported, not repaired.

    Repairing it is a re-index decision (the chunk may be stale), not a backfill.
    """
    _add_chunk(session, row_id="good", content_hash=chunk_content_hash("title", "body"))
    _add_chunk(session, row_id="drifted", content_hash="c" * 64)

    report = backfill_rag_index_identity(session, apply=False)

    assert report.contract_sample == 2
    assert report.contract_violations == 1


def test_apply_is_idempotent(session: Session) -> None:
    """A second run has nothing left to fill."""
    _add_chunk(session, row_id="a")

    backfill_rag_index_identity(session, apply=True)
    session.commit()
    second = backfill_rag_index_identity(session, apply=True)

    assert second.content_hash_targets == 0
    assert second.index_version_targets == 0
    assert second.content_hash_written == 0
    assert second.index_version_written == 0
