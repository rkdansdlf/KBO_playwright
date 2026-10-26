"""Tests for RAG vector reactivation of cleanup-tombstoned entries."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_vector import RagChunkVector
from src.repositories.vector_search_repository import VectorSearchRepository
from src.services.rag_index_propagation import _bulk_upsert_vector, publish_index_batch
from src.services.rag_vector_reactivation import (
    CLEANUP_TOMBSTONE_META_KEY,
    clear_reactivation_columns_cache,
    reactivate_cleanup_tombstoned_keys,
)


def _create_harness(include_is_active: bool = True, include_valid_to: bool = True) -> tuple[sa.Engine, sa.Engine]:
    """Create in-memory SQLite engines for sparse and vector stores."""
    clear_reactivation_columns_cache()
    sparse_engine = sa.create_engine("sqlite:///:memory:")
    RagChunk.__table__.create(sparse_engine)

    vector_engine = sa.create_engine("sqlite:///:memory:")
    RagChunkVector.__table__.create(vector_engine)
    with vector_engine.connect() as conn:
        if include_is_active:
            conn.execute(sa.text("ALTER TABLE rag_chunks ADD COLUMN is_active BOOLEAN"))
        if include_valid_to:
            conn.execute(sa.text("ALTER TABLE rag_chunks ADD COLUMN valid_to TIMESTAMP"))
        conn.commit()
    return sparse_engine, vector_engine


def test_publish_index_batch_reactivates_deleted_row_with_marker() -> None:
    """A cleanup-marked row DELETED+is_active=0+valid_to -> publish -> ACTIVE, is_active=1, valid_to NULL, marker gone."""
    sparse_engine, vector_engine = _create_harness()
    with vector_engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to, meta) "
                "VALUES (1, 'game', 'g1', 'Old Game', 'Content', 'DELETED', 0, '2026-01-01 00:00:00', :meta)"
            ),
            {"meta": json.dumps({CLEANUP_TOMBSTONE_META_KEY: True, "document_type": "game"})},
        )
        conn.commit()

    with Session(sparse_engine) as sparse_session, Session(vector_engine) as vector_session:
        publish_index_batch(
            sparse_session,
            vector_session,
            [{"source_table": "game", "source_row_id": "g1", "title": "Game 1", "content": "Updated content"}],
        )

    with vector_engine.connect() as conn:
        row = conn.execute(
            sa.text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 1")
        ).fetchone()

    assert row is not None
    assert row[0] == "ACTIVE"
    assert row[1] == 1
    assert row[2] is None
    parsed_meta = json.loads(row[3]) if isinstance(row[3], str) else row[3]
    assert CLEANUP_TOMBSTONE_META_KEY not in parsed_meta


def test_publish_index_batch_reactivates_tombstoned_row_with_marker() -> None:
    """A cleanup-marked row TOMBSTONED+is_active=0+valid_to -> publish -> ACTIVE, is_active=1, valid_to NULL, marker gone."""
    sparse_engine, vector_engine = _create_harness()
    with vector_engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to, meta) "
                "VALUES (1, 'game', 'g1', 'Old Game', 'Content', 'TOMBSTONED', 0, '2026-01-01 00:00:00', :meta)"
            ),
            {"meta": json.dumps({CLEANUP_TOMBSTONE_META_KEY: True})},
        )
        conn.commit()

    with Session(sparse_engine) as sparse_session, Session(vector_engine) as vector_session:
        publish_index_batch(
            sparse_session,
            vector_session,
            [{"source_table": "game", "source_row_id": "g1", "title": "Game 1", "content": "Updated content"}],
        )

    with vector_engine.connect() as conn:
        row = conn.execute(
            sa.text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 1")
        ).fetchone()

    assert row is not None
    assert row[0] == "ACTIVE"
    assert row[1] == 1
    assert row[2] is None
    parsed_meta = json.loads(row[3]) if isinstance(row[3], str) else row[3]
    assert CLEANUP_TOMBSTONE_META_KEY not in parsed_meta


def test_publish_index_batch_leaves_active_unmarked_time_bounded_row_untouched() -> None:
    """An ACTIVE row with is_active=0 and valid_to but NO marker -> untouched after publish."""
    sparse_engine, vector_engine = _create_harness()
    with vector_engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to, meta) "
                "VALUES (2, 'game', 'g2', 'Curated Game', 'Content', 'ACTIVE', 0, '2026-12-31 23:59:59', :meta)"
            ),
            {"meta": json.dumps({"curated": True})},
        )
        conn.commit()

    with Session(sparse_engine) as sparse_session, Session(vector_engine) as vector_session:
        publish_index_batch(
            sparse_session,
            vector_session,
            [
                {
                    "source_table": "game",
                    "source_row_id": "g2",
                    "title": "Game 2",
                    "content": "Curated new content",
                    "meta": {"curated": True},
                }
            ],
        )

    with vector_engine.connect() as conn:
        row = conn.execute(
            sa.text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 2")
        ).fetchone()

    assert row is not None
    assert row[0] == "ACTIVE"
    assert row[1] == 0  # Unmarked: remains untouched!
    assert row[2] == "2026-12-31 23:59:59"
    parsed_meta = json.loads(row[3]) if isinstance(row[3], str) else row[3]
    assert parsed_meta.get("curated") is True


def test_publish_index_batch_repeated_publish_is_noop() -> None:
    """Publish twice -> still active, second publish no-op."""
    sparse_engine, vector_engine = _create_harness()
    with vector_engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to, meta) "
                "VALUES (1, 'game', 'g1', 'Old Game', 'Content', 'DELETED', 0, '2026-01-01 00:00:00', :meta)"
            ),
            {"meta": json.dumps({CLEANUP_TOMBSTONE_META_KEY: True})},
        )
        conn.commit()

    payload = [{"source_table": "game", "source_row_id": "g1", "title": "Game 1", "content": "Updated content"}]
    with Session(sparse_engine) as sparse_session, Session(vector_engine) as vector_session:
        publish_index_batch(sparse_session, vector_session, payload)

    # First assert
    with vector_engine.connect() as conn:
        row1 = conn.execute(sa.text("SELECT index_status, is_active, valid_to FROM rag_chunks WHERE id = 1")).fetchone()
    assert row1 == ("ACTIVE", 1, None)

    # Second publish
    with Session(sparse_engine) as sparse_session, Session(vector_engine) as vector_session:
        publish_index_batch(sparse_session, vector_session, payload)

    with vector_engine.connect() as conn:
        row2 = conn.execute(sa.text("SELECT index_status, is_active, valid_to FROM rag_chunks WHERE id = 1")).fetchone()
    assert row2 == ("ACTIVE", 1, None)


def test_publish_index_batch_fixes_stuck_active_marker_row() -> None:
    """A stuck row 'ACTIVE|is_active=0|marker' is fixed by re-publishing."""
    sparse_engine, vector_engine = _create_harness()
    with vector_engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to, meta) "
                "VALUES (1, 'game', 'g1', 'Stuck Game', 'Content', 'ACTIVE', 0, '2026-01-01 00:00:00', :meta)"
            ),
            {"meta": json.dumps({CLEANUP_TOMBSTONE_META_KEY: True})},
        )
        conn.commit()

    with Session(sparse_engine) as sparse_session, Session(vector_engine) as vector_session:
        publish_index_batch(
            sparse_session,
            vector_session,
            [{"source_table": "game", "source_row_id": "g1", "title": "Game 1", "content": "Fixed content"}],
        )

    with vector_engine.connect() as conn:
        row = conn.execute(
            sa.text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 1")
        ).fetchone()

    assert row is not None
    assert row[0] == "ACTIVE"
    assert row[1] == 1
    assert row[2] is None
    parsed_meta = json.loads(row[3]) if isinstance(row[3], str) else row[3]
    assert CLEANUP_TOMBSTONE_META_KEY not in parsed_meta


def test_publish_index_batch_pending_only_failure_keeps_marker_and_deactivated() -> None:
    """PENDING-only write (publish failing at the ACTIVE step) keeps the marker and is_active=0."""
    sparse_engine, vector_engine = _create_harness()
    with vector_engine.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to, meta) "
                "VALUES (1, 'game', 'g1', 'Game', 'Content', 'DELETED', 0, '2026-01-01 00:00:00', :meta)"
            ),
            {"meta": json.dumps({CLEANUP_TOMBSTONE_META_KEY: True})},
        )
        conn.commit()

    real_upsert = VectorSearchRepository.upsert_chunk

    def failing_upsert(self: VectorSearchRepository, session: Session, chunk_data: dict) -> None:
        if chunk_data.get("index_status") == "ACTIVE":
            raise RuntimeError("Injected ACTIVE step failure")
        real_upsert(self, session, chunk_data)

    with (
        patch.object(VectorSearchRepository, "upsert_chunk", failing_upsert),
        pytest.raises(RuntimeError, match="Injected ACTIVE step failure"),
    ):
        with Session(sparse_engine) as sparse_session, Session(vector_engine) as vector_session:
            publish_index_batch(
                sparse_session,
                vector_session,
                [{"source_table": "game", "source_row_id": "g1", "title": "Game 1", "content": "Content"}],
            )

    with vector_engine.connect() as conn:
        row = conn.execute(
            sa.text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE id = 1")
        ).fetchone()

    assert row is not None
    # PENDING step was committed, ACTIVE step failed:
    assert row[0] == "PENDING"
    assert row[1] == 0
    parsed_meta = json.loads(row[3]) if isinstance(row[3], str) else row[3]
    assert parsed_meta.get(CLEANUP_TOMBSTONE_META_KEY) is True


def test_bulk_upsert_vector_compiled_sql_contains_case_marker_preservation() -> None:
    """_bulk_upsert_vector's compiled SQL (postgresql dialect) contains the CASE marker preservation."""
    session = MagicMock()
    payloads = [{"source_table": "game", "source_row_id": "g1", "title": "G1", "content": "C1"}]

    _bulk_upsert_vector(session, payloads, "PENDING")

    statement = session.execute.call_args[0][0]
    compiled_str = str(statement.compile(dialect=postgresql.dialect()))

    assert "excluded.meta ||" in compiled_str
    assert "CASE WHEN" in compiled_str
    assert "rag_chunks.meta ?" in compiled_str
    assert "AS JSONB" in compiled_str


def test_reactivate_cleanup_tombstoned_keys_partial_and_empty() -> None:
    """reactivate_cleanup_tombstoned_keys handles partial columns, empty keys, and missing columns."""
    clear_reactivation_columns_cache()
    # 1. Empty keys
    engine = sa.create_engine("sqlite:///:memory:")
    RagChunkVector.__table__.create(engine)
    with Session(engine) as session:
        assert reactivate_cleanup_tombstoned_keys(session, []) == 0

    # 2. Only is_active
    clear_reactivation_columns_cache()
    engine_active = sa.create_engine("sqlite:///:memory:")
    RagChunkVector.__table__.create(engine_active)
    with engine_active.connect() as conn:
        conn.execute(sa.text("ALTER TABLE rag_chunks ADD COLUMN is_active BOOLEAN"))
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, meta) "
                "VALUES (1, 't', 'r', 'T', 'C', 'ACTIVE', 0, :meta)"
            ),
            {"meta": json.dumps({CLEANUP_TOMBSTONE_META_KEY: True})},
        )
        conn.commit()

    with Session(engine_active) as session:
        count = reactivate_cleanup_tombstoned_keys(session, [("t", "r")])
        session.commit()
    assert count == 1

    with engine_active.connect() as conn:
        row = conn.execute(sa.text("SELECT is_active, meta FROM rag_chunks WHERE id = 1")).fetchone()
    assert row is not None
    assert row[0] == 1
    parsed_meta = json.loads(row[1]) if isinstance(row[1], str) else row[1]
    assert CLEANUP_TOMBSTONE_META_KEY not in parsed_meta
