"""Base abstract retriever interface for KBO RAG components."""

from __future__ import annotations

import abc
import asyncio
import time
from typing import Any

from src.rag.dto import RetrievalCandidate, RetrievalQuery, RetrievalResult


class BaseRetriever(abc.ABC):
    """Abstract base class for all RAG retrieval engines (Dense, Sparse, Hybrid)."""

    def __init__(self, *, name: str = "BaseRetriever") -> None:
        """Initialize the retriever."""
        self.name = name

    @abc.abstractmethod
    def retrieve(self, query: RetrievalQuery | str, **kwargs: Any) -> RetrievalResult:  # noqa: ANN401
        """Execute synchronous retrieval given a RetrievalQuery or raw query string.

        Args:
            query: RetrievalQuery object or query string.
            **kwargs: Overrides for query parameters (e.g. top_k, category, filters).

        Returns:
            RetrievalResult containing retrieved candidates and performance metrics.

        """

    async def retrieve_async(self, query: RetrievalQuery | str, **kwargs: Any) -> RetrievalResult:  # noqa: ANN401
        """Execute asynchronous retrieval.

        Default implementation offloads the synchronous retrieve to an executor.
        Subclasses with native async support may override this method.

        Args:
            query: RetrievalQuery object or query string.
            **kwargs: Overrides for query parameters.

        Returns:
            RetrievalResult containing retrieved candidates and performance metrics.

        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: self.retrieve(query, **kwargs))

    def _normalize_query(self, query: RetrievalQuery | str, **kwargs: Any) -> RetrievalQuery:  # noqa: ANN401
        """Normalize raw string or existing query into a standardized RetrievalQuery."""
        if isinstance(query, str):
            return RetrievalQuery(
                query_text=query,
                top_k=kwargs.get("top_k", 5),
                category=kwargs.get("category"),
                filters=kwargs.get("filters"),
                dense_weight=kwargs.get("dense_weight", 2.0),
                sparse_weight=kwargs.get("sparse_weight", 1.0),
                rrf_k=kwargs.get("rrf_k", 2),
                resolve_entities=kwargs.get("resolve_entities", True),
            )
        if kwargs:
            # Shallow clone with overridden parameters if kwargs supplied
            query_dict = query.to_dict()
            query_dict.update(kwargs)
            return RetrievalQuery(**query_dict)
        return query

    def _create_result(
        self,
        query: RetrievalQuery,
        candidates: list[RetrievalCandidate],
        start_time: float,
        *,
        mode: str | None = None,
        resolved_entities: dict[str, Any] | None = None,
    ) -> RetrievalResult:
        """Create a standard RetrievalResult with measured duration."""
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        return RetrievalResult(
            query=query,
            candidates=candidates,
            elapsed_ms=elapsed_ms,
            retrieval_mode=mode or self.name,
            total_matches=len(candidates),
            resolved_entities=resolved_entities or {},
        )
