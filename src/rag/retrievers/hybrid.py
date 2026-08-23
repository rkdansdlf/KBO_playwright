"""Unified Hybrid Retriever combining Dense Vector and Sparse BM25 with Reciprocal Rank Fusion (RRF)."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from src.rag.base_retriever import BaseRetriever
from src.rag.dto import RetrievalCandidate, RetrievalQuery, RetrievalResult
from src.rag.retrievers.oracle_dense import OracleDenseRetriever
from src.rag.retrievers.sparse_bm25 import SparseBM25Retriever
from src.services.kbo_entity_resolver import resolve_kbo_entities
from src.utils.kbo_entity_extractor import extract_kbo_entities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_RRF_K = 2
DEFAULT_DENSE_RRF_WEIGHT = 2.0
DEFAULT_SPARSE_RRF_WEIGHT = 1.0


class UnifiedHybridRetriever(BaseRetriever):
    """Hybrid retriever orchestrating Dense and Sparse retrieval with RRF rank fusion."""

    def __init__(
        self,
        session: Session,
        *,
        dense_retriever: OracleDenseRetriever | None = None,
        sparse_retriever: SparseBM25Retriever | None = None,
        name: str = "UnifiedHybridRetriever",
    ) -> None:
        """Initialize the unified hybrid retriever."""
        super().__init__(name=name)
        self.session = session
        self.dense_retriever = dense_retriever or OracleDenseRetriever()
        self.sparse_retriever = sparse_retriever or SparseBM25Retriever(session)

    def retrieve(self, query: RetrievalQuery | str, **kwargs: Any) -> RetrievalResult:  # noqa: ANN401
        """Execute hybrid search using Reciprocal Rank Fusion."""
        t0 = time.perf_counter()
        norm_query = self._normalize_query(query, **kwargs)

        resolved_entities_dict: dict[str, Any] = {}
        effective_filters = dict(norm_query.filters) if norm_query.filters else {}

        if norm_query.resolve_entities:
            try:
                extracted = extract_kbo_entities(norm_query.query_text)
                resolved = resolve_kbo_entities(self.session, extracted)
                resolved_entities_dict = resolved.to_dict()

                if resolved.team_id and "team_id" not in effective_filters:
                    effective_filters["team_id"] = resolved.team_id
                if resolved.player_id and "player_id" not in effective_filters:
                    effective_filters["player_id"] = resolved.player_id
                if resolved.season and "season_year" not in effective_filters:
                    effective_filters["season_year"] = resolved.season
            except Exception:
                logger.exception("Error extracting/resolving KBO entities from query: %s", norm_query.query_text)

        sub_query = RetrievalQuery(
            query_text=norm_query.query_text,
            top_k=norm_query.top_k * 2,
            category=norm_query.category,
            filters=effective_filters,
            dense_weight=norm_query.dense_weight,
            sparse_weight=norm_query.sparse_weight,
            rrf_k=norm_query.rrf_k,
            language=norm_query.language,
            resolve_entities=False,
        )

        dense_res = self.dense_retriever.retrieve(sub_query)
        sparse_res = self.sparse_retriever.retrieve(sub_query)

        # Merge results using Reciprocal Rank Fusion (RRF)
        merged_candidates = self._fuse_rrf(
            dense_candidates=dense_res.candidates,
            sparse_candidates=sparse_res.candidates,
            k=norm_query.rrf_k or DEFAULT_RRF_K,
            dense_weight=norm_query.dense_weight or DEFAULT_DENSE_RRF_WEIGHT,
            sparse_weight=norm_query.sparse_weight or DEFAULT_SPARSE_RRF_WEIGHT,
            top_k=norm_query.top_k,
        )

        return self._create_result(
            norm_query,
            merged_candidates,
            t0,
            mode="hybrid_rrf",
            resolved_entities=resolved_entities_dict,
        )

    def _fuse_rrf(  # noqa: PLR0913
        self,
        dense_candidates: list[RetrievalCandidate],
        sparse_candidates: list[RetrievalCandidate],
        k: int,
        dense_weight: float,
        sparse_weight: float,
        top_k: int,
    ) -> list[RetrievalCandidate]:
        """Perform Reciprocal Rank Fusion between dense and sparse results."""
        scores: dict[str, float] = {}
        items: dict[str, RetrievalCandidate] = {}
        dense_ranks: dict[str, int] = {}
        sparse_ranks: dict[str, int] = {}

        for rank, cand in enumerate(dense_candidates, start=1):
            cid = cand.chunk_id
            scores[cid] = scores.get(cid, 0.0) + (dense_weight / (k + rank))
            dense_ranks[cid] = rank
            items[cid] = cand

        for rank, cand in enumerate(sparse_candidates, start=1):
            cid = cand.chunk_id
            scores[cid] = scores.get(cid, 0.0) + (sparse_weight / (k + rank))
            sparse_ranks[cid] = rank
            if cid not in items:
                items[cid] = cand

        sorted_ids = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)[:top_k]

        final_candidates: list[RetrievalCandidate] = []
        for cid in sorted_ids:
            base_item = items[cid]
            final_cand = RetrievalCandidate(
                chunk_id=cid,
                title=base_item.title,
                content=base_item.content,
                score=scores[cid],
                category=base_item.category,
                source_url=base_item.source_url,
                vector_rank=dense_ranks.get(cid),
                bm25_rank=sparse_ranks.get(cid),
                source_table=base_item.source_table,
                source_row_id=base_item.source_row_id,
                team_id=base_item.team_id,
                player_id=base_item.player_id,
                season_year=base_item.season_year,
                metadata=base_item.metadata,
                provenance=base_item.provenance,
            )
            final_candidates.append(final_cand)

        return final_candidates
