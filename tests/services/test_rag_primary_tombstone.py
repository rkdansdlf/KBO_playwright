"""Tests for primary-only RAG tombstone lifecycle operations."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.rag_chunk import RagChunk
from src.services.rag_primary_tombstone import inspect_primary_tombstones, tombstone_primary_rows


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    RagChunk.__table__.create(engine)
    session = Session(engine)
    session.add(
        RagChunk(
            source_table="kbo_definitions",
            source_row_id="old_1",
            content="old",
            index_status="ACTIVE",
        ),
    )
    session.commit()
    return session


def test_inspect_primary_tombstones_is_read_only() -> None:
    """Return current status and missing identity without mutation."""
    session = _session()
    result = inspect_primary_tombstones(session, ("kbo_definitions:old_1", "game:missing"))

    assert result[0].final_status == "ACTIVE"
    assert result[1].final_status == "MISSING"
    assert session.query(RagChunk).one().index_status == "ACTIVE"
    session.close()


def test_tombstone_primary_rows_commits_two_phase_status() -> None:
    """Persist DELETE_PENDING then DELETED while leaving rows queryable as tombstones."""
    session = _session()
    result = tombstone_primary_rows(session, ("kbo_definitions:old_1",))

    assert result[0].final_status == "DELETED"
    assert session.query(RagChunk).one().index_status == "DELETED"
    session.close()
