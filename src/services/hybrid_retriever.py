"""Hybrid Retriever combining Dense Vector and Sparse BM25 Keyword Search with Reciprocal Rank Fusion (RRF)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.db.vector_engine import is_pgvector_available
from src.services.rag_search_engine import RagSearchEngine
from src.utils.kbo_entity_extractor import extract_kbo_entities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class HybridSearchResult:
    """Dataclass representing a hybrid search result item."""

    chunk_id: str
    title: str | None
    content: str
    source_url: str | None
    category: str
    vector_rank: int | None
    bm25_rank: int | None
    rrf_score: float
    meta: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert result to serializable dict."""
        return {
            "chunk_id": self.chunk_id,
            "title": self.title,
            "content": self.content,
            "source_url": self.source_url,
            "category": self.category,
            "vector_rank": self.vector_rank,
            "bm25_rank": self.bm25_rank,
            "score": round(self.rrf_score, 4),
            "meta": self.meta,
        }


def _fetch_dense_vectors(
    query: str,
    top_k: int,
    merged_filters: dict[str, Any],
    target_category: str | None,
    rank_map: dict[str, dict[str, Any]],
) -> None:
    """Query pgvector and update rank_map."""
    if not is_pgvector_available():
        return

    try:
        from src.repositories.vector_search_repository import VectorSearchRepository
        from src.services.embedding_service import EmbeddingService

        embedder = EmbeddingService()
        vector_repo = VectorSearchRepository()
        query_vector = embedder.get_embedding(query)

        vector_results = vector_repo.search_by_cosine(
            query_vector=query_vector,
            top_k=top_k * 3,
            team_id=merged_filters.get("team_id"),
            season_year=merged_filters.get("season_year"),
            source_table=merged_filters.get("source_table"),
            player_id=merged_filters.get("player_id"),
            document_type=target_category,
        )

        for rank, v_item in enumerate(vector_results, start=1):
            if not _matches_dense_filters(v_item, merged_filters):
                continue
            key = _result_key(v_item, fallback=str(v_item.get("id") or rank))
            if key in rank_map:
                rank_map[key]["vector_rank"] = rank
                if not rank_map[key]["source_url"] and v_item.get("source_url"):
                    rank_map[key]["source_url"] = v_item["source_url"]
                if not rank_map[key]["meta"] and v_item.get("meta"):
                    rank_map[key]["meta"] = v_item["meta"]
            else:
                rank_map[key] = {
                    "title": v_item.get("title"),
                    "content": v_item.get("content", ""),
                    "source_url": v_item.get("source_url"),
                    "category": v_item.get("document_type") or target_category or "general",
                    "source_table": v_item.get("source_table"),
                    "source_row_id": v_item.get("source_row_id"),
                    "bm25_rank": None,
                    "vector_rank": rank,
                    "meta": v_item.get("meta", {}),
                }
    except (RuntimeError, ValueError, OSError, TypeError, KeyError, IndexError):
        logger.warning("Dense vector retrieval failed; falling back to BM25 sparse scoring only", exc_info=True)


def _result_key(item: dict[str, Any], fallback: str) -> str:
    """Return a stable cross-index key for a RAG result."""
    source_table = item.get("source_table")
    source_row_id = item.get("source_row_id")
    if source_table and source_row_id:
        return f"{source_table}:{source_row_id}"
    return str(item.get("chunk_id") or fallback)


def _matches_dense_filters(item: dict[str, Any], filters: dict[str, Any]) -> bool:
    """Apply filters unavailable as first-class pgvector columns to dense hits."""
    text = f"{item.get('title') or ''} {item.get('content') or ''}".lower()
    for key in ("stadium", "player_name"):
        expected = filters.get(key)
        if expected and str(expected).lower() not in text:
            return False
    return True


class HybridRetriever:
    """RRF (Reciprocal Rank Fusion) Hybrid Retriever for KBO Knowledge Base."""

    def __init__(self, session: Session, k: int = 60) -> None:
        """Initialize retriever.

        Args:
            session: DB Session.
            k: RRF constant parameter (default 60).

        """
        self.session = session
        self.k = k
        self.bm25_engine = RagSearchEngine(session)

    def retrieve(
        self,
        query: str,
        top_k: int = 5,
        category: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[HybridSearchResult]:
        """Perform hybrid retrieval combining keyword matching and semantic scoring."""
        extracted = extract_kbo_entities(query)
        merged_filters = extracted.to_filters()
        if filters:
            merged_filters.update(filters)
        target_category = category or extracted.category

        # 1. BM25 Search
        bm25_chunks = self.bm25_engine.search(
            query=query,
            top_k=top_k * 3,
            category=target_category,
            filters=merged_filters,
        )
        rank_map: dict[str, dict[str, Any]] = {}

        for rank, item in enumerate(bm25_chunks, start=1):
            key = _result_key(item, fallback=str(item["chunk_id"]))
            rank_map[key] = {
                "title": item.get("title"),
                "content": item.get("content", ""),
                "source_url": item.get("source_url"),
                "category": item.get("category") or target_category or "general",
                "source_table": item.get("source_table"),
                "source_row_id": item.get("source_row_id"),
                "bm25_rank": rank,
                "vector_rank": None,
                "meta": item.get("meta", {}),
            }

        # 2. Dense Vector Search
        _fetch_dense_vectors(query, top_k, merged_filters, target_category, rank_map)

        # 3. Compute Reciprocal Rank Fusion (RRF) scores
        results: list[HybridSearchResult] = []
        for key, entry in rank_map.items():
            bm25_r = entry["bm25_rank"]
            vector_r = entry["vector_rank"]

            score = 0.0
            if bm25_r is not None:
                score += 1.0 / (self.k + bm25_r)
            if vector_r is not None:
                score += 1.0 / (self.k + vector_r)

            results.append(
                HybridSearchResult(
                    chunk_id=key,
                    title=entry.get("title"),
                    content=entry.get("content", ""),
                    source_url=entry.get("source_url"),
                    category=entry.get("category", "general"),
                    vector_rank=vector_r,
                    bm25_rank=bm25_r,
                    rrf_score=score,
                    meta=entry.get("meta", {}),
                )
            )

        results.sort(key=lambda x: x.rrf_score, reverse=True)
        return results[:top_k]
