"""Oracle AI Vector Search repository for the canonical RAG table."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, func, literal_column, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import defer

from src.db.engine import get_rag_index_session
from src.models.rag_chunk import OracleVectorType, RagChunk
from src.repositories.rag_chunk_repository import RagChunkRepository
from src.services.rag_index_identity import RETRIEVABLE_INDEX_STATUSES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement

_SEARCH_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)
_ORACLE_GLOBAL_VECTOR_SOURCES = frozenset(
    {
        "game",
        "game_lineups",
        "game_play_by_play",
        "player_basic",
        "player_movements",
        "player_season_batting",
        "player_season_pitching",
        "team_standings_daily",
    }
)
# Distance over an IN-bound ID set scales at roughly 2.7ms per row on the
# production instance, so keep the exact restricted path under ~500ms.
_ID_RESTRICTED_SEARCH_LIMIT = 200


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
                scalar_filters: list[tuple[Any, Any]] = [
                    (column, value)
                    for column, value in (
                        (RagChunk.team_id, team_id),
                        (RagChunk.season_year, season_year),
                        (RagChunk.source_table, source_table),
                        (RagChunk.league_type_code, league_type_code),
                        (RagChunk.player_id, player_id),
                        (RagChunk.index_version, index_version),
                    )
                    if value is not None
                ]
                global_source_search = source_table in _ORACLE_GLOBAL_VECTOR_SOURCES
                stmt = (
                    select(RagChunk, distance.label("distance"))
                    .options(defer(RagChunk.embedding))
                    .where(
                        RagChunk.embedding.is_not(None),
                        RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
                    )
                )
                fetch_limit = max(top_k * 20, top_k)
                scoped_search = bool(scalar_filters) and not global_source_search
                if scoped_search:
                    restricted_ids = self._matching_chunk_ids(session, scalar_filters)
                    if not restricted_ids:
                        return []
                    if len(restricted_ids) <= _ID_RESTRICTED_SEARCH_LIMIT:
                        # Distance over a bounded ID set avoids the exact full-vector
                        # scan that scalar WHERE clauses force on HNSW searches.
                        stmt = stmt.where(RagChunk.id.in_(restricted_ids))
                        fetch_limit = max(top_k * 5, top_k)
                approximate_stmt = stmt.order_by(distance).suffix_with(
                    literal_column(f"FETCH APPROX FIRST {fetch_limit} ROWS ONLY")
                )
                rows = session.execute(approximate_stmt).all()
                return self._render_rows(
                    rows,
                    filters={
                        "source_table": source_table,
                        "team_id": team_id,
                        "season_year": season_year,
                        "league_type_code": league_type_code,
                        "player_id": player_id,
                        "index_version": index_version,
                        "document_type": document_type,
                        "game_date": game_date,
                    },
                    top_k=top_k,
                )
        except _SEARCH_EXCEPTIONS:
            return []

    @staticmethod
    def _matching_chunk_ids(session: Session, scalar_filters: list[tuple[Any, Any]]) -> list[int]:
        """Collect candidate chunk IDs for the scalar filters with one bounded query."""
        id_stmt = (
            select(RagChunk.id)
            .where(
                *(column == value for column, value in scalar_filters),
                RagChunk.embedding.is_not(None),
                RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
            )
            .limit(_ID_RESTRICTED_SEARCH_LIMIT + 1)
        )
        return [int(chunk_id) for chunk_id in session.scalars(id_stmt).all()]

    @staticmethod
    def _distance_expression(query_vector: list[float]) -> ColumnElement[float]:
        """Build a typed Oracle ``VECTOR_DISTANCE`` expression."""
        query_bind = bindparam("oracle_query_vector", value=query_vector, type_=OracleVectorType())
        return func.vector_distance(RagChunk.embedding, query_bind, literal_column("COSINE"))

    @staticmethod
    def _render_rows(
        rows: list[tuple[RagChunk, float]],
        *,
        filters: dict[str, Any],
        top_k: int,
    ) -> list[dict[str, Any]]:
        """Convert Oracle rows to the shared dense-result payload."""
        source_table = filters.get("source_table")
        document_type = filters.get("document_type")
        game_date = filters.get("game_date")
        results: list[dict[str, Any]] = []
        for chunk, distance in rows:
            meta = chunk.meta or {}
            actual_document_type = meta.get("document_type") or meta.get("category")
            actual_game_date = meta.get("game_date")
            if source_table and chunk.source_table != source_table:
                continue
            for expected, key in (
                (filters.get("team_id"), "team_id"),
                (filters.get("season_year"), "season_year"),
                (filters.get("league_type_code"), "league_type_code"),
                (filters.get("player_id"), "player_id"),
                (filters.get("index_version"), "index_version"),
            ):
                actual = getattr(chunk, key, None) or meta.get(key)
                if expected is not None and str(actual) != str(expected):
                    break
            else:
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
