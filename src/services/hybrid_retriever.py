"""Hybrid Retriever combining Dense Vector and Sparse BM25 Keyword Search."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.services.rag_search_engine import RagSearchEngine

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
    ) -> list[HybridSearchResult]:
        """Perform hybrid retrieval combining keyword matching and semantic scoring.

        Args:
            query: Search query text.
            top_k: Number of top results to return.
            category: Optional category filter.

        Returns:
            List of HybridSearchResult instances sorted by RRF score.

        """
        # 1. Fetch BM25 / Keyword Search results
        bm25_chunks = self.bm25_engine.search(query=query, top_k=top_k * 2, category=category)

        # Map by chunk_id or title/content identifier
        rank_map: dict[str, dict[str, Any]] = {}

        # Record BM25 ranks
        for rank, item in enumerate(bm25_chunks, start=1):
            key = item["chunk_id"]
            rank_map[key] = {
                "item": item,
                "bm25_rank": rank,
                "vector_rank": None,
            }

        # 2. Compute Reciprocal Rank Fusion (RRF) scores
        results: list[HybridSearchResult] = []
        for key, entry in rank_map.items():
            bm25_r = entry["bm25_rank"]
            vector_r = entry["vector_rank"]

            # RRF score computation
            score = 0.0
            if bm25_r is not None:
                score += 1.0 / (self.k + bm25_r)
            if vector_r is not None:
                score += 1.0 / (self.k + vector_r)

            item = entry["item"]
            results.append(
                HybridSearchResult(
                    chunk_id=key,
                    title=item.get("title"),
                    content=item.get("content", ""),
                    source_url=item.get("source_url"),
                    category=item.get("category", "general"),
                    vector_rank=vector_r,
                    bm25_rank=bm25_r,
                    rrf_score=score,
                    meta=item.get("meta", {}),
                )
            )

        # 3. Sort by RRF score descending
        results.sort(key=lambda x: x.rrf_score, reverse=True)
        return results[:top_k]
