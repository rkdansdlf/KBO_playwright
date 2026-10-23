"""Integration tests for PostgreSQL bulk RAG vector reactivation."""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_vector import RagChunkVector
from src.services.rag_index_propagation import _can_bulk_publish, publish_index_batch
from src.services.rag_vector_reactivation import (
    CLEANUP_TOMBSTONE_META_KEY,
    clear_reactivation_columns_cache,
)

ENV_VAR = "KBO_LOCAL_PG_TEST_URL"
pytestmark = pytest.mark.skipif(not os.getenv(ENV_VAR), reason=f"{ENV_VAR} not set")


@pytest.fixture
def pg_engines() -> Generator[tuple[sa.Engine, sa.Engine]]:
    """Provision fresh sparse and vector schemas on the PostgreSQL test database."""
    url = os.environ[ENV_VAR]
    root_engine = sa.create_engine(url)

    with root_engine.connect() as conn:
        conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS test_sparse_reactivation"))
        conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS test_vector_reactivation"))
        conn.commit()

    engine_sparse = sa.create_engine(
        url,
        connect_args={"options": "-csearch_path=test_sparse_reactivation,public"},
    )
    engine_vector = sa.create_engine(
        url,
        connect_args={"options": "-csearch_path=test_vector_reactivation,public"},
    )

    RagChunk.__table__.create(engine_sparse, checkfirst=True)
    RagChunkVector.__table__.create(engine_vector, checkfirst=True)

    with engine_vector.connect() as conn:
        conn.execute(sa.text("ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS is_active boolean DEFAULT true"))
        conn.execute(sa.text("ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS valid_to timestamptz"))
        conn.commit()

    clear_reactivation_columns_cache()

    try:
        yield engine_sparse, engine_vector
    finally:
        clear_reactivation_columns_cache()
        engine_sparse.dispose()
        engine_vector.dispose()
        with root_engine.connect() as conn:
            conn.execute(sa.text("DROP SCHEMA test_sparse_reactivation CASCADE"))
            conn.execute(sa.text("DROP SCHEMA test_vector_reactivation CASCADE"))
            conn.commit()
        root_engine.dispose()


def test_bulk_pg_publish_reactivation_lifecycle(pg_engines: tuple[sa.Engine, sa.Engine]) -> None:
    """Verify PostgreSQL bulk publish path (_publish_bulk) handles cleanup tombstone markers."""
    engine_sparse, engine_vector = pg_engines

    with engine_vector.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (source_table, source_row_id, title, content, index_status, is_active, valid_to, meta) "
                "VALUES "
                "('milestone', 'm1', 'T1', 'C1', 'DELETED', false, now(), '{\"kbo_cleanup_tombstoned\": true, \"team\": \"SSG\"}'::jsonb), "
                "('milestone', 'm2', 'T2', 'C2', 'TOMBSTONED', false, now(), '{\"kbo_cleanup_tombstoned\": true}'::jsonb), "
                "('milestone', 'm3', 'T3', 'C3', 'ACTIVE', false, '2026-12-31 23:59:59+00', '{\"curated\": true}'::jsonb), "
                "('milestone', 'm4', 'T4', 'C4', 'ACTIVE', false, now(), '{\"kbo_cleanup_tombstoned\": true}'::jsonb)"
            )
        )
        conn.commit()

    payloads = [
        {
            "source_table": "milestone",
            "source_row_id": "m1",
            "title": "T1-rev",
            "content": "C1-rev",
            "meta": {"team": "SSG"},
        },
        {"source_table": "milestone", "source_row_id": "m2", "title": "T2-rev", "content": "C2-rev"},
        {
            "source_table": "milestone",
            "source_row_id": "m3",
            "title": "T3-rev",
            "content": "C3-rev",
            "meta": {"curated": True},
        },
        {"source_table": "milestone", "source_row_id": "m4", "title": "T4-rev", "content": "C4-rev"},
    ]

    with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
        assert _can_bulk_publish(s_sparse, s_vec) is True
        count = publish_index_batch(s_sparse, s_vec, payloads)
        assert count == 4

    with engine_vector.connect() as conn:
        rows = {
            r[0]: r
            for r in conn.execute(
                sa.text("SELECT source_row_id, index_status, is_active, valid_to, meta FROM rag_chunks")
            ).fetchall()
        }

    # 1. m1: DELETED+marker -> ACTIVE, is_active=True, valid_to=None, marker gone
    m1 = rows["m1"]
    assert m1[1] == "ACTIVE"
    assert m1[2] is True
    assert m1[3] is None
    assert CLEANUP_TOMBSTONE_META_KEY not in m1[4]
    assert m1[4].get("team") == "SSG"

    # 2. m2: TOMBSTONED+marker -> ACTIVE, is_active=True, valid_to=None, marker gone
    m2 = rows["m2"]
    assert m2[1] == "ACTIVE"
    assert m2[2] is True
    assert m2[3] is None
    assert CLEANUP_TOMBSTONE_META_KEY not in m2[4]

    # 3. m3: unmarked time-bounded -> untouched is_active=False and valid_to preserved
    m3 = rows["m3"]
    assert m3[1] == "ACTIVE"
    assert m3[2] is False
    assert m3[3] is not None
    assert CLEANUP_TOMBSTONE_META_KEY not in m3[4]
    assert m3[4].get("curated") is True

    # 4. m4: stuck ACTIVE+marker fixed -> ACTIVE, is_active=True, valid_to=None, marker gone
    m4 = rows["m4"]
    assert m4[1] == "ACTIVE"
    assert m4[2] is True
    assert m4[3] is None
    assert CLEANUP_TOMBSTONE_META_KEY not in m4[4]

    # 5. Repeated publish of m1 -> still active, is_active=True, valid_to=None
    with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
        publish_index_batch(s_sparse, s_vec, [payloads[0]])

    with engine_vector.connect() as conn:
        m1_repeat = conn.execute(
            sa.text("SELECT index_status, is_active, valid_to, meta FROM rag_chunks WHERE source_row_id = 'm1'")
        ).fetchone()

    assert m1_repeat is not None
    assert m1_repeat[0] == "ACTIVE"
    assert m1_repeat[1] is True
    assert m1_repeat[2] is None
    assert CLEANUP_TOMBSTONE_META_KEY not in m1_repeat[3]
