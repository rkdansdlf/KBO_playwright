"""pgvector 코사인 유사도 검색 레포지토리.

pgvector DB에 저장된 rag_chunks 테이블에서
쿼리 벡터와의 코사인 유사도를 기준으로 상위 K개 청크를 검색합니다.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError

from src.db.vector_engine import get_vector_session, is_pgvector_available
from src.models.rag_chunk_vector import RagChunkVector
from src.services.rag_index_identity import (
    ACTIVE_INDEX_STATUS,
    RETRIEVABLE_INDEX_STATUSES,
    chunk_content_hash,
    current_index_version,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_SEARCH_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError)


class VectorSearchRepository:
    """pgvector 코사인 유사도 기반 청크 검색 및 upsert."""

    def search_by_cosine(  # noqa: PLR0913 (public 검색 API 호환성 유지)
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
        """코사인 거리 기준으로 유사한 청크를 검색합니다.

        Args:
            query_vector: 검색 쿼리의 1536차원 임베딩.
            top_k: 반환할 최대 결과 수.
            team_id: 팀 코드 필터 (선택).
            season_year: 시즌 연도 필터 (선택).
            source_table: 소스 테이블 필터 (선택).
            player_id: 선수 ID 필터 (선택).
            league_type_code: 리그 레벨 코드 필터 (선택).
            document_type: 문서 유형 필터 (선택).
            game_date: 대상 날짜 필터, YYYY-MM-DD 문자열 (선택).
            index_version: 인덱스 버전 필터 (선택).

        Returns:
            유사도 점수(score, 높을수록 유사)와 청크 데이터를 담은 dict 리스트.

        """
        if not is_pgvector_available():
            logger.warning("pgvector not available — returning empty results")
            return []
        try:
            with get_vector_session() as session:
                return self._execute_search(
                    session,
                    query_vector,
                    top_k,
                    team_id,
                    season_year,
                    source_table,
                    league_type_code,
                    document_type,
                    game_date,
                    player_id,
                    index_version,
                )
        except _SEARCH_EXCEPTIONS:
            logger.exception("Vector similarity search failed")
            return []

    def _execute_search(  # noqa: PLR0913 (search_by_cosine과 동일 서명 유지)
        self,
        session: Session,
        query_vector: list[float],
        top_k: int,
        team_id: str | None,
        season_year: int | None,
        source_table: str | None,
        league_type_code: int | None,
        document_type: str | None,
        game_date: str | None,
        player_id: str | None,
        index_version: str | None,
    ) -> list[dict[str, Any]]:
        # cosine_distance: 0 = 동일, 2 = 반대 → score = 1 - distance
        distance_expr = RagChunkVector.embedding.cosine_distance(query_vector)
        score_expr = (1 - distance_expr).label("score")

        stmt = select(RagChunkVector, score_expr)

        if team_id:
            stmt = stmt.where(RagChunkVector.team_id == team_id)
        if season_year:
            stmt = stmt.where(RagChunkVector.season_year == season_year)
        if source_table:
            stmt = stmt.where(RagChunkVector.source_table == source_table)
        if player_id:
            stmt = stmt.where(RagChunkVector.player_id == player_id)
        if league_type_code is not None:
            stmt = stmt.where(RagChunkVector.league_type_code == league_type_code)
        if document_type:
            stmt = stmt.where(RagChunkVector.document_type == document_type)
        if game_date:
            stmt = stmt.where(RagChunkVector.game_date == game_date)
        if index_version:
            stmt = stmt.where(RagChunkVector.index_version == index_version)

        stmt = stmt.where(
            or_(
                RagChunkVector.index_status.is_(None),
                RagChunkVector.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
            )
        )

        # 임베딩이 NULL인 행 제외
        stmt = stmt.where(RagChunkVector.embedding.is_not(None))
        stmt = stmt.order_by(distance_expr).limit(top_k)

        results: list[dict[str, Any]] = []
        for chunk, score in session.execute(stmt):
            results.append(
                {
                    "id": chunk.id,
                    "title": chunk.title,
                    "content": chunk.content,
                    "source_table": chunk.source_table,
                    "source_row_id": chunk.source_row_id,
                    "team_id": chunk.team_id,
                    "player_id": chunk.player_id,
                    "season_year": chunk.season_year,
                    "document_type": chunk.document_type,
                    "game_date": str(chunk.game_date) if chunk.game_date else None,
                    "published_at": chunk.published_at.isoformat() if chunk.published_at else None,
                    "source_url": chunk.source_url,
                    "language": chunk.language,
                    "content_hash": chunk.content_hash,
                    "index_version": chunk.index_version,
                    "index_status": chunk.index_status,
                    "indexed_at": chunk.indexed_at.isoformat() if chunk.indexed_at else None,
                    "score": round(float(score), 4),
                    "meta": chunk.meta or {},
                }
            )
        return results

    def upsert_chunk(self, session: Session, chunk_data: dict[str, Any]) -> None:
        """단일 RAG 청크를 upsert합니다 (source_table + source_row_id 기준).

        Args:
            session: pgvector DB 세션.
            chunk_data: 청크 데이터 dict (content, source_table, source_row_id 필수).

        """
        source_table = chunk_data["source_table"]
        source_row_id = chunk_data["source_row_id"]
        title = chunk_data.get("title")
        content = chunk_data["content"]
        content_hash = chunk_data.get("content_hash") or chunk_content_hash(title, content)
        index_version = chunk_data.get("index_version") or current_index_version()
        index_status = chunk_data.get("index_status") or ACTIVE_INDEX_STATUS
        indexed_at = chunk_data.get("indexed_at")

        existing = session.scalar(
            select(RagChunkVector).where(
                RagChunkVector.source_table == source_table,
                RagChunkVector.source_row_id == source_row_id,
            )
        )

        if existing:
            existing.title = title
            existing.content = content
            existing.embedding = chunk_data.get("embedding")
            existing.meta = chunk_data.get("meta", {})
            existing.team_id = chunk_data.get("team_id")
            existing.player_id = chunk_data.get("player_id")
            existing.season_year = chunk_data.get("season_year")
            existing.document_type = chunk_data.get("document_type")
            existing.game_date = chunk_data.get("game_date")
            existing.published_at = chunk_data.get("published_at")
            existing.source_url = chunk_data.get("source_url")
            existing.language = chunk_data.get("language")
            existing.league_type_code = chunk_data.get("league_type_code")
            existing.content_hash = content_hash
            existing.index_version = index_version
            existing.index_status = index_status
            existing.indexed_at = indexed_at
        else:
            session.add(
                RagChunkVector(
                    source_table=source_table,
                    source_row_id=source_row_id,
                    title=title,
                    content=content,
                    embedding=chunk_data.get("embedding"),
                    meta=chunk_data.get("meta", {}),
                    team_id=chunk_data.get("team_id"),
                    player_id=chunk_data.get("player_id"),
                    season_year=chunk_data.get("season_year"),
                    document_type=chunk_data.get("document_type"),
                    game_date=chunk_data.get("game_date"),
                    published_at=chunk_data.get("published_at"),
                    source_url=chunk_data.get("source_url"),
                    language=chunk_data.get("language"),
                    league_type_code=chunk_data.get("league_type_code"),
                    content_hash=content_hash,
                    index_version=index_version,
                    index_status=index_status,
                    indexed_at=indexed_at,
                )
            )

    def count_chunks(self, source_table: str | None = None) -> int:
        """저장된 청크 수를 반환합니다.

        Args:
            source_table: 특정 소스 테이블로 제한 (선택).

        Returns:
            청크 총 개수.

        """
        try:
            with get_vector_session() as session:
                stmt = select(RagChunkVector)
                if source_table:
                    stmt = stmt.where(RagChunkVector.source_table == source_table)
                return (
                    session.execute(select(RagChunkVector.id).select_from(stmt.subquery())).rowcount
                    or session.query(RagChunkVector)
                    .filter(
                        RagChunkVector.source_table == source_table if source_table else RagChunkVector.id.is_not(None)
                    )
                    .count()
                )
        except _SEARCH_EXCEPTIONS:
            return 0
