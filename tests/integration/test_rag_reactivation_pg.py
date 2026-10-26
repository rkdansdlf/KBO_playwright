"""Integration tests for PostgreSQL bulk RAG vector reactivation."""

from __future__ import annotations

import io
import os
from collections.abc import Generator
from datetime import UTC, date, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.player_milestone import PlayerMilestone
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_vector import RagChunkVector
from scripts.maintenance.cleanup_corrupted_milestones import run_cleanup
from src.services.rag_index_identity import stable_milestone_source_row_id
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


def _milestone_source_session() -> Session:
    """Return an in-memory operational DB with two corrupted 2026 milestone rows."""
    engine = sa.create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    for mid, pid, name, cat in ((1, "101", "최정", "500홈런"), (2, "102", "손아섭", "2500안타")):
        session.add(
            PlayerMilestone(
                id=mid,
                season=2026,
                player_id=pid,
                player_name=name,
                team_code="SSG",
                milestone_category=cat,
                current_val=505,
                target_val=500,
                remaining_val=0,
                is_achieved=1,
                achieved_date=date(2026, 4, 15),
                created_at=datetime(2026, 3, 1, tzinfo=UTC),
                updated_at=datetime(2026, 4, 15, tzinfo=UTC),
            )
        )
    session.commit()
    return session


def test_cleanup_then_publish_keeps_curated_inactive_row_on_pg(pg_engines: tuple[sa.Engine, sa.Engine]) -> None:
    """QA #60 item 2 on PostgreSQL: cleanup + publish keep ACTIVE|false|2099 rows inactive, reactivate the rest."""
    engine_sparse, engine_vector = pg_engines
    curated = stable_milestone_source_row_id(2026, "101", "500홈런")
    normal = stable_milestone_source_row_id(2026, "102", "2500안타")
    for engine in (engine_sparse, engine_vector):
        with engine.connect() as conn:
            extra_cols = ", is_active, valid_to" if engine is engine_vector else ""
            conn.execute(
                sa.text(
                    f"INSERT INTO rag_chunks (source_table, source_row_id, title, content, index_status{extra_cols}) "
                    "VALUES ('milestone', :c, 'T', 'C', 'ACTIVE'"
                    + (", false, '2099-12-31 00:00:00+00'" if engine is engine_vector else "")
                    + "), ('milestone', :n, 'T', 'C', 'ACTIVE'"
                    + (", true, NULL" if engine is engine_vector else "")
                    + ")"
                ),
                {"c": curated, "n": normal},
            )
            conn.commit()

    source_session = _milestone_source_session()
    with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
        report = run_cleanup(source_session, s_sparse, s_vec, apply=True, expect_count=2, out=io.StringIO())
        assert report.status_code == 0
        assert report.vector_tombstoned == 2

    with engine_vector.connect() as conn:
        rows = {
            r[0]: r
            for r in conn.execute(
                sa.text("SELECT source_row_id, index_status, is_active, valid_to, meta FROM rag_chunks")
            ).fetchall()
        }
    assert rows[curated][1:3] == ("DELETED", False)
    assert rows[curated][3].year == 2099
    assert CLEANUP_TOMBSTONE_META_KEY not in rows[curated][4]
    assert rows[normal][1:3] == ("DELETED", False)
    assert rows[normal][4].get(CLEANUP_TOMBSTONE_META_KEY) is True

    payloads = [
        {"source_table": "milestone", "source_row_id": curated, "title": "T", "content": "C2"},
        {"source_table": "milestone", "source_row_id": normal, "title": "T", "content": "C2"},
    ]
    with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
        assert publish_index_batch(s_sparse, s_vec, payloads) == 2

    with engine_vector.connect() as conn:
        rows = {
            r[0]: r
            for r in conn.execute(
                sa.text("SELECT source_row_id, index_status, is_active, valid_to, meta FROM rag_chunks")
            ).fetchall()
        }
    assert rows[curated][1:3] == ("ACTIVE", False)
    assert rows[curated][3].year == 2099
    assert rows[normal][1:4] == ("ACTIVE", True, None)
    assert CLEANUP_TOMBSTONE_META_KEY not in rows[normal][4]
    source_session.close()


def test_cleanup_refuses_while_publish_pending_on_pg(pg_engines: tuple[sa.Engine, sa.Engine]) -> None:
    """QA #60 item 3 on PostgreSQL: a PENDING vector row (publish in flight) makes --apply refuse with no writes."""
    engine_sparse, engine_vector = pg_engines
    pending = stable_milestone_source_row_id(2026, "102", "2500안타")
    with engine_vector.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (source_table, source_row_id, title, content, index_status, is_active) "
                "VALUES ('milestone', :p, 'T', 'C', 'PENDING', true)"
            ),
            {"p": pending},
        )
        conn.commit()

    source_session = _milestone_source_session()
    with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
        report = run_cleanup(source_session, s_sparse, s_vec, apply=True, expect_count=2, out=io.StringIO())
    assert report.status_code == 3
    assert "vector_pending=1" in (report.error_message or "")
    assert source_session.scalar(sa.select(sa.func.count(PlayerMilestone.id))) == 2
    with engine_vector.connect() as conn:
        row = conn.execute(sa.text("SELECT index_status, is_active FROM rag_chunks")).one()
    assert tuple(row) == ("PENDING", True)
    source_session.close()
