"""Oracle AI Vector Search repository for the canonical RAG table."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, func, literal_column, select
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import get_rag_index_session
from src.models.rag_chunk import OracleVectorType, RagChunk
from src.repositories.rag_chunk_repository import RagChunkRepository
from src.services.rag_index_identity import RETRIEVABLE_INDEX_STATUSES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement

_SEARCH_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)


class OracleVectorSearchRepository:
    """Search and upsert native Oracle VECTOR values in ``rag_chunks``."""

    def search_by_cosine(  # noqa: PLR0913
        self,
        query_vector: list[float],
        top_k: int = 5,
        team_id: str | None = None,
        season_year: int | None = None,
        source_table: str | None = None,
        league_type_code: int | None = None,
        document_type: str | None = None,
        game_date: str | None = None,
        player_id: str | None = None,
        index_version: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return nearest chunks using Oracle ``VECTOR_DISTANCE`` with cosine distance."""
        try:
            with get_rag_index_session() as session:
                distance = self._distance_expression(query_vector)
                stmt = select(RagChunk, distance.label("distance")).where(
                    RagChunk.embedding.is_not(None),
                    RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
                )
                for column, value in (
                    (RagChunk.team_id, team_id),
                    (RagChunk.season_year, season_year),
                    (RagChunk.source_table, source_table),
                    (RagChunk.league_type_code, league_type_code),
                    (RagChunk.player_id, player_id),
                    (RagChunk.index_version, index_version),
                ):
                    if value is not None:
                        stmt = stmt.where(column == value)
                rows = session.execute(stmt.order_by(distance).limit(max(top_k * 3, top_k))).all()
                return self._render_rows(rows, document_type=document_type, game_date=game_date, top_k=top_k)
        except _SEARCH_EXCEPTIONS:
            return []

    @staticmethod
    def _distance_expression(query_vector: list[float]) -> ColumnElement[float]:
        """Build a typed Oracle ``VECTOR_DISTANCE`` expression."""
        query_bind = bindparam("oracle_query_vector", value=query_vector, type_=OracleVectorType())
        return func.vector_distance(RagChunk.embedding, query_bind, literal_column("COSINE"))

    @staticmethod
    def _render_rows(
        rows: list[tuple[RagChunk, float]],
        *,
        document_type: str | None,
        game_date: str | None,
        top_k: int,
    ) -> list[dict[str, Any]]:
        """Convert Oracle rows to the shared dense-result payload."""
        results: list[dict[str, Any]] = []
        for chunk, distance in rows:
            meta = chunk.meta or {}
            actual_document_type = meta.get("document_type") or meta.get("category")
            actual_game_date = meta.get("game_date")
            if document_type and actual_document_type != document_type:
                continue
            if game_date and str(actual_game_date) != str(game_date):
                continue
            results.append(
                {
                    "id": chunk.id,
                    "chunk_id": f"{chunk.source_table}:{chunk.source_row_id}",
                    "title": chunk.title,
                    "content": chunk.content,
                    "source_table": chunk.source_table,
                    "source_row_id": chunk.source_row_id,
                    "team_id": chunk.team_id or meta.get("team_id"),
                    "player_id": chunk.player_id or meta.get("player_id"),
                    "season_year": chunk.season_year or meta.get("season_year"),
                    "document_type": actual_document_type,
                    "game_date": str(actual_game_date) if actual_game_date else None,
                    "published_at": _iso_value(meta.get("published_at")),
                    "source_url": meta.get("source_url"),
                    "language": meta.get("language"),
                    "content_hash": chunk.content_hash,
                    "index_version": chunk.index_version,
                    "index_status": chunk.index_status,
                    "indexed_at": _iso_value(chunk.indexed_at),
                    "score": round(1.0 - float(distance), 4),
                    "meta": meta,
                },
            )
            if len(results) >= top_k:
                break
        return results

    def upsert_chunk(self, session: Session, chunk_data: dict[str, Any]) -> None:
        """Upsert one Oracle vector row through the canonical RAG repository."""
        RagChunkRepository(session).upsert_chunks(session, [chunk_data])

    def count_chunks(self, source_table: str | None = None) -> int:
        """Return the number of Oracle vector rows."""
        try:
            with get_rag_index_session() as session:
                stmt = select(RagChunk.id).where(RagChunk.embedding.is_not(None))
                if source_table:
                    stmt = stmt.where(RagChunk.source_table == source_table)
                return len(session.execute(stmt).all())
        except _SEARCH_EXCEPTIONS:
            return 0


def _iso_value(value: object) -> str | None:
    """Serialize date-like metadata values without changing strings."""
    return value.isoformat() if hasattr(value, "isoformat") else value if isinstance(value, str) else None
