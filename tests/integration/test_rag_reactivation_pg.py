"""Integration tests for PostgreSQL bulk RAG vector reactivation."""

from __future__ import annotations

import contextlib
import io
import os
import threading
import time
from collections.abc import Generator
from datetime import UTC, date, datetime
from typing import Any

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


# ---------------------------------------------------------------------------
# PR #62 regression tests: real PG concurrency for SELECT/UPDATE race
# ---------------------------------------------------------------------------


def _seed_sparse_active(engine: sa.Engine, keys: list[str]) -> None:
    """Insert milestone sparse rows as ACTIVE."""
    with engine.connect() as conn:
        for key in keys:
            conn.execute(
                sa.text(
                    "INSERT INTO rag_chunks (source_table, source_row_id, title, content, "
                    "index_status, meta) "
                    "VALUES ('milestone', :k, 'T', 'C', 'ACTIVE', '{}'::jsonb)"
                ),
                {"k": key},
            )
        conn.commit()


def _seed_vector_active(engine: sa.Engine, keys: list[str]) -> None:
    """Insert milestone vector rows as ACTIVE with is_active=true."""
    with engine.connect() as conn:
        for key in keys:
            conn.execute(
                sa.text(
                    "INSERT INTO rag_chunks (source_table, source_row_id, title, content, "
                    "index_status, is_active, meta) "
                    "VALUES ('milestone', :k, 'T', 'C', 'ACTIVE', true, '{}'::jsonb)"
                ),
                {"k": key},
            )
        conn.commit()


@pytest.mark.parametrize("bulk", [True, False], ids=["bulk", "non-bulk"])
def test_pr62_vector_pending_race_cleanup_reports_error_on_pg(
    pg_engines: tuple[sa.Engine, sa.Engine],
    monkeypatch: pytest.MonkeyPatch,
    bulk: bool,
) -> None:
    """Cleanup must not silently tombstone a key when publish commits PENDING between SELECT and UPDATE."""
    import scripts.maintenance.cleanup_corrupted_milestones as cleanup_mod
    import src.services.rag_index_propagation as prop_mod

    engine_sparse, engine_vector = pg_engines

    if not bulk:
        monkeypatch.setattr(prop_mod, "_can_bulk_publish", lambda *_a, **_k: False)

    key1 = stable_milestone_source_row_id(2026, "101", "500홈런")
    key2 = stable_milestone_source_row_id(2026, "102", "2500안타")
    race_key = key1

    # Seed both keys as ACTIVE in sparse and vector
    _seed_sparse_active(engine_sparse, [key1, key2])
    _seed_vector_active(engine_vector, [key1, key2])

    source_session = _milestone_source_session()

    # Events for thread synchronization
    pending_committed = threading.Event()
    release_publisher = threading.Event()
    publisher_error: list[BaseException] = []
    pub_thread: threading.Thread | None = None

    original_fetch = cleanup_mod._fetch_vector_chunk_row

    def racing_fetch(
        session: Session,
        source_table: str,
        source_row_id: str,
        **kw: Any,
    ) -> tuple[dict[str, Any] | None, bool]:
        result = original_fetch(session, source_table, source_row_id, **kw)
        nonlocal pub_thread
        if source_row_id == race_key and not pending_committed.is_set():
            # Start publisher thread
            pub_thread = threading.Thread(target=_publisher, daemon=True)
            pub_thread.start()
            # Wait for the publisher's first commit (PENDING)
            if not pending_committed.wait(timeout=15):
                raise TimeoutError("Publisher did not commit PENDING in time")
        return result

    def _publisher() -> None:
        try:
            s_sparse = Session(engine_sparse)
            s_vector = Session(engine_vector)

            orig_commit = s_vector.commit
            call_count = [0]

            def instrumented_commit() -> None:
                orig_commit()
                call_count[0] += 1
                if call_count[0] == 1:
                    # First commit wrote PENDING -> signal main thread
                    pending_committed.set()
                    # Wait for main thread to finish cleanup's UPDATE
                    if not release_publisher.wait(timeout=15):
                        raise TimeoutError("Main thread did not release publisher in time")

            s_vector.commit = instrumented_commit  # type: ignore[assignment]

            payload = {
                "source_table": "milestone",
                "source_row_id": race_key,
                "title": "INFLIGHT",
                "content": "inflight content for " + race_key,
                "meta": {"player_name": "최정"},
                "embedding": [0.01] * 1536,
            }
            publish_index_batch(s_sparse, s_vector, [payload])
            s_sparse.close()
            s_vector.close()
        except BaseException as exc:
            publisher_error.append(exc)
            pending_committed.set()  # unblock main thread on error

    monkeypatch.setattr(cleanup_mod, "_fetch_vector_chunk_row", racing_fetch)

    out = io.StringIO()
    try:
        with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
            report = run_cleanup(
                source_session,
                s_sparse,
                s_vec,
                apply=True,
                expect_count=2,
                out=out,
            )
    finally:
        release_publisher.set()
        if pub_thread is not None:
            pub_thread.join(timeout=15)

    assert not publisher_error, f"Publisher thread failed: {publisher_error[0]}"

    with engine_vector.connect() as conn:
        row = conn.execute(
            sa.text("SELECT index_status, is_active FROM rag_chunks WHERE source_row_id = :k"),
            {"k": race_key},
        ).one()
    assert tuple(row) == ("ACTIVE", True)

    output = out.getvalue()
    assert report.status_code == 1, f"Expected error exit but got {report.status_code}. Output:\n{output}"
    assert "PENDING (in-flight publish); skipped" in output
    assert report.vector_tombstoned == 1, f"Expected 1 tombstoned key, got {report.vector_tombstoned}"

    source_session.close()


def test_pr62_legacy_vector_pending_race_on_pg(
    pg_engines: tuple[sa.Engine, sa.Engine],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy vector row flipped to PENDING between inspection and UPDATE must not be tombstoned."""
    import scripts.maintenance.cleanup_corrupted_milestones as cleanup_mod

    engine_sparse, engine_vector = pg_engines

    key1 = stable_milestone_source_row_id(2026, "101", "500홈런")
    key2 = stable_milestone_source_row_id(2026, "102", "2500안타")

    # Seed keyed chunks
    _seed_sparse_active(engine_sparse, [key1, key2])
    _seed_vector_active(engine_vector, [key1, key2])

    # Seed a legacy empty-key vector chunk matching 최정/500홈런
    with engine_vector.connect() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO rag_chunks (source_table, source_row_id, title, content, "
                "index_status, is_active, meta) "
                "VALUES ('milestone', '', 'T', 'legacy', 'ACTIVE', true, "
                '\'{"player_name": "최정", "milestone_category": "500홈런"}\'::jsonb)'
            )
        )
        conn.commit()

    with engine_vector.connect() as conn:
        legacy_id = conn.execute(
            sa.text("SELECT id FROM rag_chunks WHERE source_row_id = '' AND source_table = 'milestone'")
        ).scalar()

    source_session = _milestone_source_session()
    original_record = cleanup_mod._record_chunk_prior_state

    def racing_record(
        source_session_arg: Session,
        *,
        store: str,
        chunk_ref: str,
        **kw: Any,
    ) -> int | None:
        result = original_record(source_session_arg, store=store, chunk_ref=chunk_ref, **kw)
        if store == "vector" and chunk_ref == f"legacy:id={legacy_id}":
            with engine_vector.connect() as conn:
                conn.execute(
                    sa.text("UPDATE rag_chunks SET index_status = 'PENDING' WHERE id = :id"),
                    {"id": legacy_id},
                )
                conn.commit()
        return result

    monkeypatch.setattr(cleanup_mod, "_record_chunk_prior_state", racing_record)

    out = io.StringIO()
    with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
        report = run_cleanup(
            source_session,
            s_sparse,
            s_vec,
            apply=True,
            expect_count=2,
            out=out,
        )

    output = out.getvalue()
    assert report.status_code == 1, f"Expected error exit but got {report.status_code}. Output:\n{output}"
    assert report.legacy_vector_tombstoned == 0
    assert "PENDING (in-flight publish); skipped" in output
    assert "row changed after read" in output

    # Legacy chunk must still be PENDING
    with engine_vector.connect() as conn:
        row = conn.execute(sa.text("SELECT index_status FROM rag_chunks WHERE id = :id"), {"id": legacy_id}).one()
    assert row[0] == "PENDING"

    source_session.close()


def test_pr62_vector_update_blocks_on_uncommitted_pending_on_pg(
    pg_engines: tuple[sa.Engine, sa.Engine],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup's UPDATE blocks on an uncommitted PENDING row; after commit, re-checked WHERE matches 0 rows."""
    import scripts.maintenance.cleanup_corrupted_milestones as cleanup_mod

    engine_sparse, engine_vector = pg_engines

    key1 = stable_milestone_source_row_id(2026, "101", "500홈런")
    key2 = stable_milestone_source_row_id(2026, "102", "2500안타")
    race_key = key1

    _seed_sparse_active(engine_sparse, [key1, key2])
    _seed_vector_active(engine_vector, [key1, key2])

    source_session = _milestone_source_session()

    raw_conn = engine_vector.connect()
    trans: sa.engine.Transaction | None = None
    saw_waiter = False
    poller_error: list[BaseException] = []
    poller_thread: threading.Thread | None = None
    started = False

    def poll_waiter_and_commit() -> None:
        nonlocal saw_waiter
        deadline = time.monotonic() + 10.0
        try:
            with engine_vector.connect().execution_options(isolation_level="AUTOCOMMIT") as poll_conn:
                while time.monotonic() < deadline:
                    cnt = poll_conn.execute(
                        sa.text(
                            "SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype IN ('transactionid', 'tuple')"
                        )
                    ).scalar()
                    if cnt and cnt > 0:
                        saw_waiter = True
                        break
                    time.sleep(0.05)
        except BaseException as exc:
            poller_error.append(exc)
        finally:
            # Commit even on timeout so nothing hangs
            try:
                if trans is not None and trans.is_active:
                    trans.commit()
            except BaseException as exc:
                poller_error.append(exc)

    original_fetch = cleanup_mod._fetch_vector_chunk_row

    def racing_fetch(
        session: Session,
        source_table: str,
        source_row_id: str,
        **kw: Any,
    ) -> tuple[dict[str, Any] | None, bool]:
        result = original_fetch(session, source_table, source_row_id, **kw)
        nonlocal started, trans, poller_thread
        if source_row_id == race_key and not started:
            started = True
            trans = raw_conn.begin()
            raw_conn.execute(
                sa.text("UPDATE rag_chunks SET index_status = 'PENDING' WHERE source_row_id = :k"),
                {"k": race_key},
            )
            poller_thread = threading.Thread(target=poll_waiter_and_commit, daemon=True)
            poller_thread.start()
        return result

    monkeypatch.setattr(cleanup_mod, "_fetch_vector_chunk_row", racing_fetch)

    out = io.StringIO()
    try:
        with Session(engine_sparse) as s_sparse, Session(engine_vector) as s_vec:
            report = run_cleanup(
                source_session,
                s_sparse,
                s_vec,
                apply=True,
                expect_count=2,
                out=out,
            )
    finally:
        if poller_thread is not None:
            poller_thread.join(timeout=10)
        if trans is not None and trans.is_active:
            with contextlib.suppress(Exception):
                trans.rollback()
        raw_conn.close()

    assert not poller_error, f"Poller thread failed: {poller_error[0]}"
    assert saw_waiter is True, "Expected cleanup's UPDATE to block on uncommitted PENDING row lock"

    output = out.getvalue()
    assert report.status_code == 1, f"Expected error exit but got {report.status_code}. Output:\n{output}"
    assert "PENDING (in-flight publish); skipped" in output
    assert "row changed after read" in output
    assert report.vector_tombstoned == 1

    # Row in PG must stay PENDING
    with engine_vector.connect() as conn:
        row = conn.execute(
            sa.text("SELECT index_status, is_active FROM rag_chunks WHERE source_row_id = :k"),
            {"k": race_key},
        ).one()
    assert row[0] == "PENDING"

    source_session.close()
