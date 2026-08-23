"""Sparse BM25 Keyword Retriever for KBO knowledge base chunks."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from src.rag.base_retriever import BaseRetriever
from src.rag.dto import RetrievalCandidate, RetrievalQuery, RetrievalResult
from src.services.rag_search_engine import RagSearchEngine

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class SparseBM25Retriever(BaseRetriever):
    """Sparse retriever utilizing BM25 keyword matching."""

    def __init__(
        self,
        session: Session,
        *,
        search_engine: RagSearchEngine | None = None,
        name: str = "SparseBM25Retriever",
    ) -> None:
        """Initialize the sparse retriever with a database session."""
        super().__init__(name=name)
        self.session = session
        self.search_engine = search_engine or RagSearchEngine(session)

    def retrieve(self, query: RetrievalQuery | str, **kwargs: object) -> RetrievalResult:
        """Execute BM25 keyword search over knowledge chunks."""
        t0 = time.perf_counter()
        norm_query = self._normalize_query(query, **kwargs)

        try:
            results = self.search_engine.search(
                query=norm_query.query_text,
                top_k=norm_query.top_k,
                category=norm_query.category,
                filters=norm_query.filters,
            )
        except Exception:
            logger.exception("Error executing sparse BM25 search for query: %s", norm_query.query_text)
            results = []

        candidates: list[RetrievalCandidate] = []
        for rank, item in enumerate(results, start=1):
            cand = RetrievalCandidate(
                chunk_id=str(item.get("id") or item.get("chunk_id") or f"sparse_{rank}"),
                title=item.get("title"),
                content=item.get("content", ""),
                score=float(item.get("score", 0.0)),
                category=item.get("category", norm_query.category or "general"),
                source_url=item.get("source_url"),
                vector_rank=None,
                bm25_rank=rank,
                source_table=item.get("source_table"),
                source_row_id=item.get("source_row_id"),
                team_id=item.get("team_id"),
                player_id=item.get("player_id"),
                season_year=item.get("season_year"),
                metadata=item.get("meta", {}),
            )
            candidates.append(cand)

        return self._create_result(norm_query, candidates, t0, mode="sparse_bm25")
