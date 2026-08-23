"""DB-backed behavior tests for the single-store RAG index audit."""

from __future__ import annotations

from datetime import datetime, timezone, UTC

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.models.rag_chunk import RagChunk
from src.services.rag_index_consistency import audit_single_store_session


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    RagChunk.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    s = factory()
    try:
        yield s
    finally:
        s.close()
        engine.dispose()


_INSERT_SQL = text(
    "INSERT INTO rag_chunks (source_table, source_row_id, title, content, content_hash, "
    "index_version, index_status, embedding_vector, indexed_at, created_at, updated_at) "
    "VALUES (:source_table, :source_row_id, :title, :content, :content_hash, "
    ":index_version, :index_status, :embedding_json, :indexed_at, :created_at, :updated_at)"
)


def _add_chunk(
    session,
    *,
    source_row_id: str,
    content_hash: str | None = "a" * 64,
    index_version: str | None = "rag-v1",
    index_status: str = "ACTIVE",
    embedding: list[float] | None = None,
    source_table: str = "games",
) -> None:
    """Insert one chunk with a real SQL NULL embedding when absent.

    ORM inserts on SQLite persist ``None`` JSON embeddings as the literal text
    ``'null'``, which defeats the ``IS NOT NULL`` projection under test.
    """
    now = datetime.now(UTC).isoformat()
    session.execute(
        _INSERT_SQL,
        {
            "source_table": source_table,
            "source_row_id": source_row_id,
            "title": "chunk",
            "content": "body",
            "content_hash": content_hash,
            "index_version": index_version,
            "index_status": index_status,
            "embedding_json": "[0.1, 0.2]" if embedding is not None else None,
            "indexed_at": now,
            "created_at": now,
            "updated_at": now,
        },
    )


def test_clean_store_reports_no_findings(session) -> None:
    for i in range(3):
        _add_chunk(session, source_row_id=f"row-{i}", embedding=[0.1])
    session.commit()

    report = audit_single_store_session(session)

    assert report.primary_count == 3
    assert report.vector_count == 3
    assert report.total_keys == 3
    assert report.findings == ()
    assert report.stale_keys == ()
    assert report.deleted_keys == ()
    assert report.is_consistent is True


def test_null_or_empty_content_hash_is_reported(session) -> None:
    _add_chunk(session, source_row_id="null-hash", content_hash=None, embedding=[0.1])
    _add_chunk(session, source_row_id="empty-hash", content_hash="", embedding=[0.1])
    session.commit()

    report = audit_single_store_session(session)

    issues = {f.source_key: f.issue for f in report.findings}
    assert issues["games:null-hash"] == "CONTENT_HASH_MISSING"
    assert issues["games:empty-hash"] == "CONTENT_HASH_MISSING"


def test_missing_index_version_is_reported(session) -> None:
    _add_chunk(session, source_row_id="no-version", index_version=None, embedding=[0.1])
    session.commit()

    report = audit_single_store_session(session)

    assert [f.issue for f in report.findings] == ["INDEX_VERSION_MISSING"]
    assert report.findings[0].source_key == "games:no-version"


def test_active_row_without_embedding_is_reported(session) -> None:
    _add_chunk(session, source_row_id="no-vector", embedding=None)
    session.commit()

    report = audit_single_store_session(session)

    assert [f.issue for f in report.findings] == ["VECTOR_EMBEDDING_MISSING"]
    assert report.to_dict()["embedding_missing"] == 1


def test_deleted_rows_may_lack_embeddings_and_land_in_deleted_keys(session) -> None:
    _add_chunk(session, source_row_id="gone", index_status="DELETED")
    _add_chunk(session, source_row_id="tomb", index_status="TOMBSTONED")
    session.commit()

    report = audit_single_store_session(session)

    assert report.findings == ()
    assert report.deleted_keys == ("games:gone", "games:tomb")
    assert report.stale_keys == ()
    assert report.is_consistent is True


def test_purged_rows_are_neither_stale_nor_deleted_keys_but_skip_embedding_check(session) -> None:
    _add_chunk(session, source_row_id="purged", index_status="PURGED")
    session.commit()

    report = audit_single_store_session(session)

    assert report.findings == ()
    assert report.deleted_keys == ()
    assert report.stale_keys == ()


def test_stale_status_blocks_consistency(session) -> None:
    _add_chunk(session, source_row_id="old", index_status="STALE", embedding=[0.1])
    _add_chunk(session, source_row_id="reindex", index_status="REINDEX_REQUIRED", embedding=[0.1])
    session.commit()

    report = audit_single_store_session(session)

    assert report.stale_keys == ("games:old", "games:reindex")
    assert report.findings == ()
    assert report.is_consistent is False


def test_one_row_can_emit_multiple_findings(session) -> None:
    _add_chunk(session, source_row_id="broken", content_hash=None, index_version=None)
    session.commit()

    report = audit_single_store_session(session)

    issues = sorted(f.issue for f in report.findings)
    assert issues == ["CONTENT_HASH_MISSING", "INDEX_VERSION_MISSING", "VECTOR_EMBEDDING_MISSING"]
