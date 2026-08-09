"""RAG(Retrieval-Augmented Generation) 서비스.

자연어 쿼리를 임베딩으로 변환하고 pgvector에서 유사한 KBO 지식 청크를 검색합니다.
LLM 답변 생성은 포함하지 않으며, 검색 결과(컨텍스트) 반환에 집중합니다.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from src.db.vector_engine import is_pgvector_available
from src.repositories.vector_search_repository import VectorSearchRepository
from src.services.embedding_service import EmbeddingService

logger = logging.getLogger(__name__)

_PGVECTOR_UNAVAILABLE_MSG = (
    "pgvector DB를 사용할 수 없습니다. "
    "docker-compose up pgvector -d 로 서비스를 기동하세요."
)

_MAX_CACHE_SIZE = 128


@dataclass
class RagResult:
    """단일 RAG 검색 결과."""

    title: str | None
    content: str
    score: float
    source_table: str
    source_row_id: str
    team_id: str | None = None
    player_id: str | None = None
    season_year: int | None = None
    document_type: str | None = None
    game_date: str | None = None
    published_at: str | None = None
    source_url: str | None = None
    language: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """직렬화 가능한 dict로 변환합니다."""
        return {
            "title": self.title,
            "content": self.content,
            "score": self.score,
            "source_table": self.source_table,
            "source_row_id": self.source_row_id,
            "team_id": self.team_id,
            "player_id": self.player_id,
            "season_year": self.season_year,
            "document_type": self.document_type,
            "game_date": self.game_date,
            "published_at": self.published_at,
            "source_url": self.source_url,
            "language": self.language,
            "meta": self.meta,
        }


class RagService:
    """쿼리 임베딩 → pgvector 검색 → 결과 반환 오케스트레이터."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        self._embedding_service = EmbeddingService()
        self._search_repo = VectorSearchRepository()

    def search(
        self,
        query: str,
        top_k: int = 5,
        filters: dict[str, Any] | None = None,
    ) -> tuple[list[RagResult], dict[str, float]]:
        """KBO 지식 베이스에서 의미적으로 유사한 청크를 검색합니다.

        Args:
            query: 자연어 검색 쿼리.
            top_k: 반환할 최대 결과 수 (기본값: 5).
            filters: 필터 dict. 지원 키: team_id, season_year, source_table.

        Returns:
            (results, timings) 튜플.
            - results: RagResult 리스트 (score 내림차순 정렬).
            - timings: embedding_ms, search_ms 포함 성능 측정값.

        Raises:
            RuntimeError: pgvector DB에 연결할 수 없는 경우.

        """
        if not is_pgvector_available():
            raise RuntimeError(_PGVECTOR_UNAVAILABLE_MSG)

        filters = filters or {}
        timings: dict[str, float] = {}

        # 0단계: LRU 캐시 키 확인
        cache_key = (query.strip(), top_k, str(sorted(filters.items())))
        if hasattr(self, "_cache") and cache_key in self._cache:
            cached_results, _ = self._cache[cache_key]
            logger.info("RAG search [CACHE HIT]: query=%r top_k=%d", query[:60], top_k)
            return cached_results, {"embedding_ms": 0.0, "search_ms": 0.0, "cache_hit": 1.0}

        # 1단계: 쿼리 임베딩 생성
        t0 = time.perf_counter()
        query_vector = self._embedding_service.get_embedding(query)
        timings["embedding_ms"] = round((time.perf_counter() - t0) * 1000, 1)

        # 2단계: pgvector 코사인 유사도 검색
        t1 = time.perf_counter()
        raw_results = self._search_repo.search_by_cosine(
            query_vector=query_vector,
            top_k=top_k,
            team_id=filters.get("team_id"),
            season_year=filters.get("season_year"),
            source_table=filters.get("source_table"),
            league_type_code=filters.get("league_type_code"),
            document_type=filters.get("document_type"),
            game_date=filters.get("game_date"),
        )
        timings["search_ms"] = round((time.perf_counter() - t1) * 1000, 1)

        results = [
            RagResult(
                title=r["title"],
                content=r["content"],
                score=r["score"],
                source_table=r["source_table"],
                source_row_id=r["source_row_id"],
                team_id=r.get("team_id"),
                player_id=r.get("player_id"),
                season_year=r.get("season_year"),
                meta=r.get("meta", {}),
                document_type=r.get("document_type"),
                game_date=r.get("game_date"),
                published_at=r.get("published_at"),
                source_url=r.get("source_url"),
                language=r.get("language"),
            )
            for r in raw_results
        ]

        logger.info(
            "RAG search: query=%r top_k=%d results=%d embed=%.1fms search=%.1fms",
            query[:60],
            top_k,
            len(results),
            timings["embedding_ms"],
            timings["search_ms"],
        )

        if not hasattr(self, "_cache"):
            self._cache = {}
        if len(self._cache) >= _MAX_CACHE_SIZE:
            self._cache.pop(next(iter(self._cache)))
        self._cache[cache_key] = (results, timings)

        return results, timings
