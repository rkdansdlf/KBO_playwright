"""Propagate RAG source updates and deletes across sparse and vector indexes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.constants import KST
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_vector import RagChunkVector
from src.repositories.rag_chunk_repository import RagChunkRepository
from src.repositories.vector_search_repository import VectorSearchRepository
from src.services.rag_index_identity import chunk_content_hash, current_index_version

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class IndexPropagationResult:
    """Describe a source mutation applied to both indexes."""

    source_key: str
    operation: str
    primary_status: str
    vector_status: str
    content_hash: str | None = None
    index_version: str | None = None
    missing_primary: bool = False
    missing_vector: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Serialize the propagation result."""
        return {
            "source_key": self.source_key,
            "operation": self.operation,
            "primary_status": self.primary_status,
            "vector_status": self.vector_status,
            "content_hash": self.content_hash,
            "index_version": self.index_version,
            "missing_primary": self.missing_primary,
            "missing_vector": self.missing_vector,
        }


def propagate_index_update(  # noqa: PLR0913
    primary_session: Session,
    vector_session: Session,
    chunk_data: dict[str, Any],
    embedding: list[float],
    *,
    index_version: str | None = None,
    indexed_at: datetime | None = None,
) -> IndexPropagationResult:
    """Update one source row in both indexes, keeping partial writes non-retrievable."""
    source_table = str(chunk_data["source_table"])
    source_row_id = str(chunk_data["source_row_id"])
    source_key = f"{source_table}:{source_row_id}"
    version = index_version or current_index_version()
    timestamp = indexed_at or datetime.now(KST)
    content_hash = chunk_content_hash(chunk_data.get("title"), chunk_data["content"])
    payload = dict(chunk_data)
    payload.update(
        {
            "embedding": embedding,
            "content_hash": content_hash,
            "index_version": version,
            "indexed_at": timestamp,
        }
    )

    _reject_purged_rows(primary_session, vector_session, source_table, source_row_id)
    publish_index_batch(primary_session, vector_session, [payload])
    return IndexPropagationResult(source_key, "update", "ACTIVE", "ACTIVE", content_hash, version)


def publish_index_batch(
    primary_session: Session,
    vector_session: Session,
    payloads: list[dict[str, Any]],
) -> int:
    """Publish a batch through a non-retrievable pending state into both indexes."""
    if not payloads:
        return 0
    for payload in payloads:
        _reject_purged_rows(
            primary_session,
            vector_session,
            str(payload["source_table"]),
            str(payload["source_row_id"]),
        )
    pending_payloads = [dict(payload, index_status="PENDING") for payload in payloads]
    sparse_repo = RagChunkRepository()
    sparse_repo.upsert_chunks(primary_session, pending_payloads, commit=False)
    vector_repo = VectorSearchRepository()
    for payload in pending_payloads:
        vector_repo.upsert_chunk(vector_session, payload)
    vector_session.commit()

    active_payloads = [dict(payload, index_status="ACTIVE") for payload in payloads]
    for payload in active_payloads:
        vector_repo.upsert_chunk(vector_session, payload)
    vector_session.commit()
    sparse_repo.upsert_chunks(primary_session, active_payloads, commit=False)
    primary_session.commit()
    return len(payloads)


def propagate_index_delete(
    primary_session: Session,
    vector_session: Session,
    source_table: str,
    source_row_id: str,
    *,
    purge: bool = False,
) -> IndexPropagationResult:
    """Mark both index rows deleted, optionally purging after the tombstone step."""
    source_key = f"{source_table}:{source_row_id}"
    primary_row = _row(primary_session, RagChunk, source_table, source_row_id)
    vector_row = _row(vector_session, RagChunkVector, source_table, source_row_id)
    if primary_row is None and vector_row is None:
        return IndexPropagationResult(
            source_key,
            "delete",
            "MISSING",
            "MISSING",
            missing_primary=True,
            missing_vector=True,
        )
    _reject_purged_rows(primary_session, vector_session, source_table, source_row_id)

    if primary_row is not None:
        primary_row.index_status = "DELETE_PENDING"
    if vector_row is not None:
        vector_row.index_status = "DELETE_PENDING"
    primary_session.commit()
    vector_session.commit()

    if primary_row is not None:
        primary_row.index_status = "DELETED"
    if vector_row is not None:
        vector_row.index_status = "DELETED"
    primary_session.commit()
    vector_session.commit()

    if purge:
        if primary_row is not None:
            primary_session.delete(primary_row)
        if vector_row is not None:
            vector_session.delete(vector_row)
        primary_session.commit()
        vector_session.commit()

    return IndexPropagationResult(
        source_key,
        "purge" if purge else "delete",
        "DELETED" if primary_row is not None else "MISSING",
        "DELETED" if vector_row is not None else "MISSING",
        missing_primary=primary_row is None,
        missing_vector=vector_row is None,
    )


def _row(session: Session, model: type[object], source_table: str, source_row_id: str) -> object | None:
    return session.scalar(
        select(model).where(
            model.source_table == source_table,
            model.source_row_id == source_row_id,
        )
    )


def _reject_purged_rows(
    primary_session: Session,
    vector_session: Session,
    source_table: str,
    source_row_id: str,
) -> None:
    for session, model in ((primary_session, RagChunk), (vector_session, RagChunkVector)):
        row = _row(session, model, source_table, source_row_id)
        if row is not None and getattr(row, "index_status", None) == "PURGED":
            message = f"Cannot mutate purged RAG index row: {source_table}:{source_row_id}"
            raise ValueError(message)
