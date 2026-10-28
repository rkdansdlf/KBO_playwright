"""Repository for managing RAG chunks in the Oracle primary database."""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, insert, select, update

from src.constants import KST
from src.db.engine import get_rag_index_session
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_term import RagChunkTerm
from src.services.rag_index_identity import ACTIVE_INDEX_STATUS, chunk_content_hash, current_index_version
from src.services.rag_sparse_terms import build_term_rows

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _parse_game_date(value: object) -> date | None:
    """Parse game date value into date object safely."""
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _parse_published_at(value: object) -> datetime | None:
    """Parse published_at timestamp safely."""
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    if isinstance(value, datetime):
        return value
    return None


def _extract_chunk_metadata(chunk_data: dict[str, Any]) -> dict[str, Any]:
    """Extract and normalize metadata dictionary for a RAG chunk."""
    meta = dict(chunk_data.get("meta") or {})
    for meta_key in ("document_type", "source_url", "language", "game_date"):
        if chunk_data.get(meta_key) is not None:
            meta_value = chunk_data[meta_key]
            if hasattr(meta_value, "isoformat"):
                meta_value = meta_value.isoformat()
            meta.setdefault(meta_key, meta_value)
    return meta


def _populate_rag_chunk(  # noqa: PLR0913
    chunk: RagChunk,
    *,
    title: str | None,
    content: str,
    embedding: object | None,
    meta: dict[str, Any],
    season_year: int | None,
    season_id: int | None,
    league_type_code: int | None,
    team_id: str | None,
    player_id: str | None,
    document_type: str | None,
    game_date: date | None,
    published_at: datetime | None,
    source_url: str | None,
    language: str | None,
    content_hash: str | None,
    index_version: str | None,
    index_status: str,
    indexed_at: datetime,
    updated_at: datetime,
) -> None:
    """Set properties on a RagChunk instance."""
    chunk.title = title
    chunk.content = content
    chunk.embedding = embedding
    chunk.meta = meta
    chunk.season_year = season_year
    chunk.season_id = season_id
    chunk.league_type_code = league_type_code
    chunk.team_id = team_id
    chunk.player_id = player_id
    chunk.document_type = document_type
    chunk.game_date = game_date
    chunk.published_at = published_at
    chunk.source_url = source_url
    chunk.language = language
    chunk.content_hash = content_hash
    chunk.index_version = index_version
    chunk.index_status = index_status
    chunk.indexed_at = indexed_at
    chunk.updated_at = updated_at


class RagChunkRepository:
    """Data Access Object (DAO) for managing rag_chunks."""

    def __init__(self, session: Session | None = None) -> None:
        """Initialize."""
        self.session = session

    def upsert_chunks(
        self,
        chunks_or_session: list[dict[str, Any]] | Session,
        chunks: list[dict[str, Any]] | None = None,
    ) -> int:
        """Save or updates RAG chunks using a clean, database-agnostic query-and-upsert approach.

        Args:
            chunks_or_session: Chunks, or a legacy explicit session followed by chunks.
            chunks: Chunks when using the legacy explicit-session call form.

        """
        if chunks is None:
            payloads = chunks_or_session
            session = self.session
        else:
            session = chunks_or_session
            payloads = chunks
        if not isinstance(payloads, list):
            message = "RAG chunks must be provided as a list"
            raise TypeError(message)
        if session is None:
            with get_rag_index_session() as managed_session:
                return self._upsert_chunks(managed_session, payloads)
        return self._upsert_chunks(session, payloads)

    def _upsert_chunks(
        self,
        session: Session,
        chunks: list[dict[str, Any]],
    ) -> int:
        """Upsert chunks into an explicitly managed session."""
        upserted_count = 0
        terms_enabled = self._term_index_enabled(session)

        now = datetime.now(KST)

        for chunk_data in chunks:
            title = chunk_data.get("title", "")
            content = chunk_data.get("content", "")
            meta = _extract_chunk_metadata(chunk_data)
            embedding = chunk_data.get("embedding")

            source_table = chunk_data.get("source_table", meta.get("source_table", meta.get("category", "unknown")))
            source_row_id = chunk_data.get("source_row_id", meta.get("source_row_id", ""))
            content_hash = chunk_data.get("content_hash") or chunk_content_hash(title, content)
            index_version = chunk_data.get("index_version") or current_index_version()
            index_status = chunk_data.get("index_status") or ACTIVE_INDEX_STATUS

            season_year = chunk_data.get("season_year", meta.get("season_year"))
            season_id = chunk_data.get("season_id", meta.get("season_id"))
            league_type_code = chunk_data.get("league_type_code", meta.get("league_type_code"))
            team_id = chunk_data.get("team_id", meta.get("team_id"))
            player_id = chunk_data.get("player_id", meta.get("player_id"))

            document_type = chunk_data.get("document_type", meta.get("document_type"))
            game_date = _parse_game_date(chunk_data.get("game_date", meta.get("game_date")))
            published_at = _parse_published_at(chunk_data.get("published_at", meta.get("published_at")))
            source_url = chunk_data.get("source_url", meta.get("source_url"))
            language = chunk_data.get("language", meta.get("language"))

            # Check if chunk exists by source_table & source_row_id
            stmt = select(RagChunk).where(
                RagChunk.source_table == source_table,
                RagChunk.source_row_id == source_row_id,
            )
            managed_chunk = session.scalar(stmt)

            if managed_chunk is None:
                managed_chunk = RagChunk(
                    source_table=source_table,
                    source_row_id=source_row_id,
                    content=content,
                    created_at=now,
                )
                session.add(managed_chunk)

            _populate_rag_chunk(
                managed_chunk,
                title=title,
                content=content,
                embedding=embedding,
                meta=meta,
                season_year=season_year,
                season_id=season_id,
                league_type_code=league_type_code,
                team_id=team_id,
                player_id=player_id,
                document_type=document_type,
                game_date=game_date,
                published_at=published_at,
                source_url=source_url,
                language=language,
                content_hash=content_hash,
                index_version=index_version,
                index_status=index_status,
                indexed_at=now,
                updated_at=now,
            )

            if terms_enabled:
                session.flush()
                self._sync_term_rows(session, managed_chunk)

            upserted_count += 1
            if upserted_count % 100 == 0:
                session.flush()

        session.flush()
        return upserted_count

    @staticmethod
    def _term_index_enabled(session: Session) -> bool:
        """Return whether Oracle term postings should be maintained for this session."""
        if os.getenv("RAG_ORACLE_SPARSE_MODE", "terms").strip().lower() != "terms":
            return False
        dialect = getattr(session.get_bind(), "dialect", None)
        return getattr(dialect, "name", None) == "oracle"

    @staticmethod
    def _sync_term_rows(session: Session, chunk: RagChunk) -> None:
        """Synchronize one chunk's postings without replacing identical primary keys."""
        chunk_id = int(chunk.id)
        rows = build_term_rows(
            chunk_id,
            chunk.title,
            chunk.content,
            chunk.meta if isinstance(chunk.meta, dict) else None,
            source_table=str(chunk.source_table),
        )
        existing_tokens = set(
            session.scalars(select(RagChunkTerm.token).where(RagChunkTerm.rag_chunk_id == chunk_id)).all(),
        )
        new_tokens = {str(row["token"]) for row in rows}
        stale_tokens = existing_tokens - new_tokens
        if stale_tokens:
            session.execute(
                delete(RagChunkTerm).where(
                    RagChunkTerm.rag_chunk_id == chunk_id,
                    RagChunkTerm.token.in_(stale_tokens),
                ),
            )

        for row in rows:
            if row["token"] in existing_tokens:
                session.execute(
                    update(RagChunkTerm)
                    .where(
                        RagChunkTerm.rag_chunk_id == chunk_id,
                        RagChunkTerm.token == row["token"],
                    )
                    .values(
                        source_table=row["source_table"],
                        term_count=row["term_count"],
                        title_count=row["title_count"],
                        game_date=row["game_date"],
                    ),
                )

        inserts = [row for row in rows if row["token"] not in existing_tokens]
        if inserts:
            session.execute(insert(RagChunkTerm), inserts)
