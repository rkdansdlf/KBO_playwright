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


def _session_dialect_name(session: Session) -> str:
    """Return the bound SQLAlchemy dialect name for a session."""
    bind = session.get_bind()
    return str(getattr(getattr(bind, "dialect", None), "name", ""))


def _can_bulk_publish(primary_session: Session, vector_session: Session) -> bool:
    """Return whether both RAG stores support PostgreSQL bulk upserts."""
    primary_dialect = _session_dialect_name(primary_session)
    vector_dialect = _session_dialect_name(vector_session)
    return primary_dialect == "postgresql" and vector_dialect == "postgresql"


def _normalized_meta(payload: dict[str, Any]) -> dict[str, Any]:
    """Build sparse metadata using the same defaults as the repository path."""
    meta = dict(payload.get("meta") or {})
    for key in ("document_type", "source_url", "language", "game_date"):
        value = payload.get(key)
        if value is not None:
            if hasattr(value, "isoformat"):
                value = value.isoformat()
            meta.setdefault(key, value)
    return meta


def _sparse_rows(payloads: list[dict[str, Any]], index_status: str) -> list[dict[str, Any]]:
    """Convert payloads to rows for the PostgreSQL sparse index."""
    now = datetime.now(KST)
    return [_sparse_row(payload, index_status, now) for payload in payloads]


def _sparse_row(payload: dict[str, Any], index_status: str, now: datetime) -> dict[str, Any]:
    """Convert one payload to a sparse index row."""
    content = payload.get("content", "")
    return {
        "season_year": payload.get("season_year"),
        "season_id": payload.get("season_id"),
        "league_type_code": payload.get("league_type_code"),
        "team_id": payload.get("team_id"),
        "player_id": payload.get("player_id"),
        "source_table": str(payload["source_table"]),
        "source_row_id": str(payload["source_row_id"]),
        "title": payload.get("title", ""),
        "content": content,
        "content_hash": payload.get("content_hash") or chunk_content_hash(payload.get("title"), content),
        "index_version": payload.get("index_version") or current_index_version(),
        "index_status": index_status,
        "indexed_at": payload.get("indexed_at") or now,
        "embedding": payload.get("embedding"),
        "meta": _normalized_meta(payload),
        "created_at": now,
        "updated_at": now,
    }


def _vector_rows(payloads: list[dict[str, Any]], index_status: str) -> list[dict[str, Any]]:
    """Convert payloads to rows for the PostgreSQL vector index."""
    now = datetime.now(KST)
    return [_vector_row(payload, index_status, now) for payload in payloads]


def _vector_row(payload: dict[str, Any], index_status: str, now: datetime) -> dict[str, Any]:
    """Convert one payload to a vector index row."""
    content = payload.get("content", "")
    return {
        "season_year": payload.get("season_year"),
        "season_id": payload.get("season_id"),
        "league_type_code": payload.get("league_type_code"),
        "team_id": payload.get("team_id"),
        "player_id": payload.get("player_id"),
        "source_table": str(payload["source_table"]),
        "source_row_id": str(payload["source_row_id"]),
        "title": payload.get("title"),
        "content": content,
        "document_type": payload.get("document_type"),
        "game_date": payload.get("game_date"),
        "published_at": payload.get("published_at"),
        "source_url": payload.get("source_url"),
        "language": payload.get("language"),
        "content_hash": payload.get("content_hash") or chunk_content_hash(payload.get("title"), content),
        "index_version": payload.get("index_version") or current_index_version(),
        "index_status": index_status,
        "indexed_at": payload.get("indexed_at") or now,
        "embedding": payload.get("embedding"),
        "meta": payload.get("meta") or {},
        "created_at": now,
        "updated_at": now,
    }


def _bulk_upsert_sparse(session: Session, payloads: list[dict[str, Any]], index_status: str) -> None:
    """Bulk upsert sparse rows using PostgreSQL's conflict-aware insert."""
    from sqlalchemy.dialects.postgresql import insert

    statement = insert(RagChunk).values(_sparse_rows(payloads, index_status))
    update_columns = {
        column: getattr(statement.excluded, column)
        for column in (
            "season_year",
            "season_id",
            "league_type_code",
            "team_id",
            "player_id",
            "title",
            "content",
            "content_hash",
            "index_version",
            "index_status",
            "indexed_at",
            "embedding_vector",
            "meta",
            "updated_at",
        )
    }
    session.execute(
        statement.on_conflict_do_update(
            index_elements=[RagChunk.source_table, RagChunk.source_row_id],
            set_=update_columns,
        )
    )


def _bulk_upsert_vector(session: Session, payloads: list[dict[str, Any]], index_status: str) -> None:
    """Bulk upsert vector rows using PostgreSQL's conflict-aware insert."""
    from sqlalchemy.dialects.postgresql import insert

    statement = insert(RagChunkVector).values(_vector_rows(payloads, index_status))
    update_columns = {
        column: getattr(statement.excluded, column)
        for column in (
            "season_year",
            "season_id",
            "league_type_code",
            "team_id",
            "player_id",
            "title",
            "content",
            "document_type",
            "game_date",
            "published_at",
            "source_url",
            "language",
            "content_hash",
            "index_version",
            "index_status",
            "indexed_at",
            "embedding",
            "meta",
            "updated_at",
        )
    }
    session.execute(
        statement.on_conflict_do_update(
            index_elements=[RagChunkVector.source_table, RagChunkVector.source_row_id],
            set_=update_columns,
        )
    )


def _reject_purged_batch(primary_session: Session, vector_session: Session, payloads: list[dict[str, Any]]) -> None:
    """Reject mutations for purged identities with one query per store."""
    from sqlalchemy import tuple_

    keys = [(str(payload["source_table"]), str(payload["source_row_id"])) for payload in payloads]
    for session, model in ((primary_session, RagChunk), (vector_session, RagChunkVector)):
        purged = session.execute(
            select(model.source_table, model.source_row_id).where(
                tuple_(model.source_table, model.source_row_id).in_(keys),
                model.index_status == "PURGED",
            )
        ).first()
        if purged:
            message = f"Cannot mutate purged RAG index row: {purged[0]}:{purged[1]}"
            raise ValueError(message)


def _publish_bulk(primary_session: Session, vector_session: Session, payloads: list[dict[str, Any]]) -> int:
    """Publish one PostgreSQL batch through pending and active states."""
    pending_payloads = [dict(payload, index_status="PENDING") for payload in payloads]
    _bulk_upsert_sparse(primary_session, pending_payloads, "PENDING")
    _bulk_upsert_vector(vector_session, pending_payloads, "PENDING")
    vector_session.commit()

    active_payloads = [dict(payload, index_status="ACTIVE") for payload in payloads]
    _bulk_upsert_vector(vector_session, active_payloads, "ACTIVE")
    vector_session.commit()
    _bulk_upsert_sparse(primary_session, active_payloads, "ACTIVE")
    primary_session.commit()
    return len(payloads)


def publish_index_batch(
    primary_session: Session,
    vector_session: Session,
    payloads: list[dict[str, Any]],
) -> int:
    """Publish a batch through a non-retrievable pending state into both indexes."""
    if not payloads:
        return 0
    if _can_bulk_publish(primary_session, vector_session):
        _reject_purged_batch(primary_session, vector_session, payloads)
        return _publish_bulk(primary_session, vector_session, payloads)
    for payload in payloads:
        _reject_purged_rows(
            primary_session,
            vector_session,
            str(payload["source_table"]),
            str(payload["source_row_id"]),
        )
    pending_payloads = [dict(payload, index_status="PENDING") for payload in payloads]
    sparse_repo = RagChunkRepository()
    sparse_repo.upsert_chunks(primary_session, pending_payloads)
    vector_repo = VectorSearchRepository()
    for payload in pending_payloads:
        vector_repo.upsert_chunk(vector_session, payload)
    vector_session.commit()

    active_payloads = [dict(payload, index_status="ACTIVE") for payload in payloads]
    for payload in active_payloads:
        vector_repo.upsert_chunk(vector_session, payload)
    vector_session.commit()
    sparse_repo.upsert_chunks(primary_session, active_payloads)
    primary_session.commit()
    return len(payloads)


def publish_single_store_batch(session: Session, payloads: list[dict[str, Any]]) -> int:
    """Publish a batch into Oracle's single sparse/vector ``rag_chunks`` table."""
    if not payloads:
        return 0
    for payload in payloads:
        _reject_single_store_purged_row(session, str(payload["source_table"]), str(payload["source_row_id"]))

    repository = RagChunkRepository(session)
    pending_payloads = [dict(payload, index_status="PENDING") for payload in payloads]
    repository.upsert_chunks(session, pending_payloads)

    active_payloads = [dict(payload, index_status="ACTIVE") for payload in payloads]
    repository.upsert_chunks(session, active_payloads)
    session.commit()
    return len(payloads)


def _reject_single_store_purged_row(session: Session, source_table: str, source_row_id: str) -> None:
    """Reject updates for rows explicitly purged from the Oracle index."""
    row = _row(session, RagChunk, source_table, source_row_id)
    if row is not None and getattr(row, "index_status", None) == "PURGED":
        message = f"Cannot mutate purged RAG index row: {source_table}:{source_row_id}"
        raise ValueError(message)


def propagate_single_store_update(
    session: Session,
    chunk_data: dict[str, Any],
    embedding: list[float],
    *,
    index_version: str | None = None,
    indexed_at: datetime | None = None,
) -> IndexPropagationResult:
    """Update one Oracle row through the single-store lifecycle."""
    source_table = str(chunk_data["source_table"])
    source_row_id = str(chunk_data["source_row_id"])
    version = index_version or current_index_version()
    timestamp = indexed_at or datetime.now(KST)
    payload = dict(chunk_data)
    payload.update({"embedding": embedding, "index_version": version, "indexed_at": timestamp})
    publish_single_store_batch(session, [payload])
    return IndexPropagationResult(
        f"{source_table}:{source_row_id}",
        "update",
        "ACTIVE",
        "ACTIVE",
        chunk_content_hash(chunk_data.get("title"), chunk_data["content"]),
        version,
    )


def propagate_single_store_delete(
    session: Session,
    source_table: str,
    source_row_id: str,
    *,
    purge: bool = False,
) -> IndexPropagationResult:
    """Mark or purge one Oracle RAG row."""
    row = _row(session, RagChunk, source_table, source_row_id)
    source_key = f"{source_table}:{source_row_id}"
    if row is None:
        return IndexPropagationResult(
            source_key,
            "delete",
            "MISSING",
            "MISSING",
            missing_primary=True,
            missing_vector=True,
        )
    _reject_single_store_purged_row(session, source_table, source_row_id)
    row.index_status = "DELETE_PENDING"
    session.commit()
    row.index_status = "DELETED"
    session.commit()
    if purge:
        session.delete(row)
        session.commit()
    return IndexPropagationResult(source_key, "purge" if purge else "delete", "DELETED", "DELETED")


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
