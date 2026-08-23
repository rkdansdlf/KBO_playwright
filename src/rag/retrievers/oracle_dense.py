"""Dense vector retriever using Oracle AI Vector Search (or local pgvector fallback)."""

from __future__ import annotations

import logging
import time

from src.db.vector_engine import is_oracle_vector_backend, is_pgvector_available
from src.rag.base_retriever import BaseRetriever
from src.rag.dto import RetrievalCandidate, RetrievalQuery, RetrievalResult
from src.repositories.vector_search_repository import VectorSearchRepository
from src.services.embedding_service import EmbeddingService

logger = logging.getLogger(__name__)


def _build_dense_repository() -> object:
    """Build the repository for the active vector backend."""
    if is_oracle_vector_backend():
        from src.repositories.oracle_vector_search_repository import OracleVectorSearchRepository

        return OracleVectorSearchRepository()
    return VectorSearchRepository()


class OracleDenseRetriever(BaseRetriever):
    """Dense vector retriever querying Oracle AI Vector Search."""

    def __init__(
        self,
        *,
        embedding_service: EmbeddingService | None = None,
        vector_repo: object | None = None,
        name: str = "OracleDenseRetriever",
    ) -> None:
        """Initialize the dense retriever."""
        super().__init__(name=name)
        self.embedding_service = embedding_service or EmbeddingService()
        self.vector_repo = vector_repo or _build_dense_repository()

    def is_available(self) -> bool:
        """Check if vector search backend is available."""
        return is_oracle_vector_backend() or is_pgvector_available()

    def retrieve(self, query: RetrievalQuery | str, **kwargs: object) -> RetrievalResult:
        """Execute dense vector similarity retrieval."""
        t0 = time.perf_counter()
        norm_query = self._normalize_query(query, **kwargs)

        if not self.is_available():
            logger.warning("Vector search backend unavailable; returning empty dense results.")
            return self._create_result(norm_query, [], t0, mode="dense_vector")

        embedding = self.embedding_service.get_embedding(norm_query.query_text)
        if not embedding:
            logger.warning("Failed to generate embedding for query: %s", norm_query.query_text)
            return self._create_result(norm_query, [], t0, mode="dense_vector")

        try:
            chunks = self.vector_repo.search_similar(
                query_vector=embedding,
                top_k=norm_query.top_k,
                category=norm_query.category,
                filters=norm_query.filters,
            )
        except Exception:
            logger.exception("Error executing dense vector search for query: %s", norm_query.query_text)
            chunks = []

        candidates: list[RetrievalCandidate] = []
        for rank, chunk in enumerate(chunks, start=1):
            cand = RetrievalCandidate(
                chunk_id=str(chunk.get("chunk_id") or chunk.get("source_row_id") or f"dense_{rank}"),
                title=chunk.get("title"),
                content=chunk.get("content", ""),
                score=float(chunk.get("similarity", chunk.get("score", 0.0))),
                category=chunk.get("category", norm_query.category or "general"),
                source_url=chunk.get("source_url"),
                vector_rank=rank,
                bm25_rank=None,
                source_table=chunk.get("source_table"),
                source_row_id=chunk.get("source_row_id"),
                team_id=chunk.get("team_id"),
                player_id=chunk.get("player_id"),
                season_year=chunk.get("season_year"),
                metadata=chunk.get("meta", {}),
            )
            candidates.append(cand)

        return self._create_result(norm_query, candidates, t0, mode="dense_vector")
