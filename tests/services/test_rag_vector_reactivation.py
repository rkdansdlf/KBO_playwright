"""Tests for RAG vector reactivation of tombstoned entries."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import Session

from src.models.rag_chunk_vector import RagChunkVector
from src.repositories.vector_search_repository import VectorSearchRepository
from src.services.rag_vector_reactivation import (
    clear_reactivation_columns_cache,
    find_deleted_vector_keys,
    reactivate_vector_chunk_by_id,
    reactivate_vector_chunks_by_keys,
)


def _create_engine_with_schema(include_is_active: bool = True, include_valid_to: bool = True) -> sa.Engine:
    """Create an in-memory SQLite engine with the rag_chunks schema and optional tombstone columns."""
    clear_reactivation_columns_cache()
    engine = sa.create_engine("sqlite:///:memory:")
    RagChunkVector.__table__.create(engine)
    with engine.connect() as conn:
        if include_is_active:
            conn.execute(sa.text("ALTER TABLE rag_chunks ADD COLUMN is_active BOOLEAN"))
        if include_valid_to:
            conn.execute(sa.text("ALTER TABLE rag_chunks ADD COLUMN valid_to TIMESTAMP"))
        conn.commit()
    return engine


def test_upsert_chunk_reactivates_deleted_row_with_tombstones() -> None:
    """A DELETED row with is_active=0 and valid_to set is re-upserted ACTIVE -> is_active=1, valid_to NULL."""
    engine = _create_engine_with_schema(include_is_active=True, include_valid_to=True)
    with engine.connect() as conn:
        conn.execute(
            sa.text("""
                INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to)
                VALUES (1, 'game', 'g1', 'Old Game', 'Content', 'DELETED', 0, '2026-01-01 00:00:00')
            """)
        )
        conn.commit()

    repo = VectorSearchRepository()
    with Session(engine) as session:
        repo.upsert_chunk(
            session,
            {
                "source_table": "game",
                "source_row_id": "g1",
                "title": "Reactivated Game",
                "content": "Reactivated Content",
                "index_status": "ACTIVE",
            },
        )
        session.commit()

    with engine.connect() as conn:
        row = conn.execute(sa.text("SELECT index_status, is_active, valid_to FROM rag_chunks WHERE id = 1")).fetchone()

    assert row is not None
    assert row[0] == "ACTIVE"
    assert row[1] == 1  # is_active set to true (1 in SQLite)
    assert row[2] is None  # valid_to cleared to NULL


def test_upsert_chunk_leaves_active_row_with_intentional_valid_to_untouched() -> None:
    """A row that was ACTIVE with an intentional valid_to and is_active=0 is upserted -> both untouched."""
    engine = _create_engine_with_schema(include_is_active=True, include_valid_to=True)
    with engine.connect() as conn:
        conn.execute(
            sa.text("""
                INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to)
                VALUES (2, 'game', 'g2', 'Curated Game', 'Content', 'ACTIVE', 0, '2026-12-31 23:59:59')
            """)
        )
        conn.commit()

    repo = VectorSearchRepository()
    with Session(engine) as session:
        repo.upsert_chunk(
            session,
            {
                "source_table": "game",
                "source_row_id": "g2",
                "title": "Updated Curated Game",
                "content": "New Content",
                "index_status": "ACTIVE",
            },
        )
        session.commit()

    with engine.connect() as conn:
        row = conn.execute(sa.text("SELECT index_status, is_active, valid_to FROM rag_chunks WHERE id = 2")).fetchone()

    assert row is not None
    assert row[0] == "ACTIVE"
    # Never touched on non-deleted rows:
    assert row[1] == 0
    assert row[2] == "2026-12-31 23:59:59"


def test_upsert_chunk_table_without_columns_no_error() -> None:
    """A table without the tombstone columns raises no error on upsert_chunk."""
    engine = _create_engine_with_schema(include_is_active=False, include_valid_to=False)
    with engine.connect() as conn:
        conn.execute(
            sa.text("""
                INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status)
                VALUES (1, 'game', 'g1', 'Title', 'Content', 'DELETED')
            """)
        )
        conn.commit()

    repo = VectorSearchRepository()
    with Session(engine) as session:
        repo.upsert_chunk(
            session,
            {
                "source_table": "game",
                "source_row_id": "g1",
                "title": "Updated",
                "content": "Updated Content",
                "index_status": "ACTIVE",
            },
        )
        session.commit()

    with engine.connect() as conn:
        row = conn.execute(sa.text("SELECT index_status FROM rag_chunks WHERE id = 1")).fetchone()

    assert row is not None
    assert row[0] == "ACTIVE"


def test_helper_functions_find_and_reactivate_deleted_keys_only() -> None:
    """find_deleted_vector_keys and reactivate_vector_chunks_by_keys clear only deleted identities."""
    engine = _create_engine_with_schema(include_is_active=True, include_valid_to=True)
    with engine.connect() as conn:
        conn.execute(
            sa.text("""
                INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active, valid_to)
                VALUES (1, 'game', 'g1', 'G1', 'C1', 'DELETED', 0, '2026-01-01 00:00:00'),
                       (2, 'game', 'g2', 'G2', 'C2', 'ACTIVE', 0, '2026-01-01 00:00:00'),
                       (3, 'player', 'p1', 'P1', 'C3', 'DELETED', 0, '2026-05-01 00:00:00')
            """)
        )
        conn.commit()

    payload_keys = [("game", "g1"), ("game", "g2"), ("player", "p1")]
    with Session(engine) as session:
        deleted_keys = find_deleted_vector_keys(session, payload_keys)
        assert set(deleted_keys) == {("game", "g1"), ("player", "p1")}

        # Simulate conflict update to ACTIVE
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    "UPDATE rag_chunks SET index_status = 'ACTIVE' WHERE (source_table, source_row_id) IN (VALUES ('game', 'g1'), ('player', 'p1'))"
                )
            )
            conn.commit()

        # Reactivate only the deleted keys
        reactivate_vector_chunks_by_keys(session, deleted_keys)
        session.commit()

    with engine.connect() as conn:
        rows = conn.execute(
            sa.text("SELECT source_table, source_row_id, index_status, is_active, valid_to FROM rag_chunks ORDER BY id")
        ).fetchall()

    # g1 (was DELETED) -> reactivated
    assert rows[0] == ("game", "g1", "ACTIVE", 1, None)
    # g2 (was ACTIVE) -> untouched
    assert rows[1] == ("game", "g2", "ACTIVE", 0, "2026-01-01 00:00:00")
    # p1 (was DELETED) -> reactivated
    assert rows[2] == ("player", "p1", "ACTIVE", 1, None)


def test_helper_functions_table_without_columns_no_error() -> None:
    """Helper functions execute cleanly when rag_chunks lacks tombstone columns."""
    engine = _create_engine_with_schema(include_is_active=False, include_valid_to=False)
    with engine.connect() as conn:
        conn.execute(
            sa.text("""
                INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status)
                VALUES (1, 'game', 'g1', 'G1', 'C1', 'DELETED')
            """)
        )
        conn.commit()

    with Session(engine) as session:
        deleted = find_deleted_vector_keys(session, [("game", "g1")])
        assert deleted == [("game", "g1")]
        # Must not raise
        reactivate_vector_chunks_by_keys(session, deleted)
        reactivate_vector_chunk_by_id(session, 1)


def test_helper_functions_partial_columns() -> None:
    """Only existing columns among is_active/valid_to are updated."""
    # Test only is_active
    engine_active = _create_engine_with_schema(include_is_active=True, include_valid_to=False)
    with engine_active.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, is_active) VALUES (1, 't', 'r', 'T', 'C', 'DELETED', 0)"
            )
        )
        conn.commit()

    with Session(engine_active) as session:
        reactivate_vector_chunks_by_keys(session, [("t", "r")])
        session.commit()

    with engine_active.connect() as conn:
        val = conn.execute(sa.text("SELECT is_active FROM rag_chunks WHERE id = 1")).scalar()
    assert val == 1

    # Test only valid_to
    engine_valid_to = _create_engine_with_schema(include_is_active=False, include_valid_to=True)
    with engine_valid_to.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (id, source_table, source_row_id, title, content, index_status, valid_to) VALUES (1, 't', 'r', 'T', 'C', 'DELETED', '2026-01-01')"
            )
        )
        conn.commit()

    with Session(engine_valid_to) as session:
        reactivate_vector_chunks_by_keys(session, [("t", "r")])
        session.commit()

    with engine_valid_to.connect() as conn:
        val = conn.execute(sa.text("SELECT valid_to FROM rag_chunks WHERE id = 1")).scalar()
    assert val is None


def test_helper_functions_empty_keys_no_op() -> None:
    """Empty key sequences return without queries or errors."""
    engine = _create_engine_with_schema(include_is_active=True, include_valid_to=True)
    with Session(engine) as session:
        assert find_deleted_vector_keys(session, []) == []
        reactivate_vector_chunks_by_keys(session, [])


def test_bulk_upsert_vector_clears_tombstones_only_for_previously_deleted_keys(monkeypatch) -> None:
    """_bulk_upsert_vector resets is_active/valid_to only for keys that were DELETED before an ACTIVE upsert."""
    from unittest.mock import MagicMock

    from src.services import rag_index_propagation as propagation

    reactivated: list[list[tuple[str, str]]] = []
    monkeypatch.setattr(propagation, "find_deleted_vector_keys", lambda _session, _keys: [("milestone", "m1")])
    monkeypatch.setattr(
        propagation, "reactivate_vector_chunks_by_keys", lambda _session, keys: reactivated.append(list(keys))
    )
    payloads = [
        {"source_table": "milestone", "source_row_id": "m1", "title": "t1", "content": "c1"},
        {"source_table": "milestone", "source_row_id": "m2", "title": "t2", "content": "c2"},
    ]

    propagation._bulk_upsert_vector(MagicMock(), payloads, "ACTIVE")
    assert reactivated == [[("milestone", "m1")]]

    reactivated.clear()
    propagation._bulk_upsert_vector(MagicMock(), payloads, "DELETE_PENDING")
    assert reactivated == []
