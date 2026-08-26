"""Hybrid Retriever combining Dense Vector and Sparse BM25 Keyword Search with Reciprocal Rank Fusion (RRF)."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from src.db.vector_engine import is_oracle_vector_backend, is_pgvector_available
from src.services.kbo_entity_resolver import resolve_kbo_entities
from src.services.rag_search_engine import RagSearchEngine
from src.utils.kbo_entity_extractor import extract_kbo_entities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
BM25_RRF_WEIGHT = 1.0
DEFAULT_RRF_K = 2
DEFAULT_DENSE_RRF_WEIGHT = 2.0
_SOURCE_FILTERS_WITHOUT_RELIABLE_TEAM_SEASON = {
    "game",
    "game_highlights",
    "game_play_by_play",
}


class EmbeddingProvider(Protocol):
    """Minimal embedding contract required by dense retrieval."""

    def get_embedding(self, text: str) -> list[float]:
        """Return one embedding for a query string."""


def _dense_repository() -> object:
    """Build the repository for the configured dense backend."""
    if is_oracle_vector_backend():
        from src.repositories.oracle_vector_search_repository import OracleVectorSearchRepository

        return OracleVectorSearchRepository()
    from src.repositories.vector_search_repository import VectorSearchRepository

    return VectorSearchRepository()


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
    provenance: dict[str, Any] | None = None

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
            "provenance": self.provenance,
        }


def _search_dense(
    query: str,
    top_k: int,
    merged_filters: dict[str, Any],
    target_category: str | None,
    embedding_service: EmbeddingProvider | None = None,
) -> tuple[list[dict[str, Any]], float, float]:
    """Query the configured dense backend without touching fusion state.

    Safe to run on a worker thread: the repository opens its own database
    session, so no caller-owned SQLAlchemy session is shared.
    """
    started_wall = time.perf_counter()
    if not is_pgvector_available() and not is_oracle_vector_backend():
        return [], 0.0, 0.0

    try:
        from src.services.embedding_service import EmbeddingService

        embedder = embedding_service or EmbeddingService()
        vector_repo = _dense_repository()
        embedding_started = time.perf_counter()
        query_vector = embedder.get_embedding(query)
        embedding_ms = round((time.perf_counter() - embedding_started) * 1000, 3)

        vector_kwargs: dict[str, Any] = {
            "query_vector": query_vector,
            "top_k": top_k if is_oracle_vector_backend() else top_k * 3,
            "team_id": merged_filters.get("team_id"),
            "season_year": merged_filters.get("season_year"),
            "source_table": merged_filters.get("source_table"),
            "player_id": merged_filters.get("player_id"),
            "document_type": target_category,
            "game_date": merged_filters.get("game_date"),
        }
        if merged_filters.get("index_version"):
            vector_kwargs["index_version"] = merged_filters["index_version"]
        vector_results = vector_repo.search_by_cosine(**vector_kwargs)
        elapsed_ms = round((time.perf_counter() - started_wall) * 1000, 3)
    except (RuntimeError, ValueError, OSError, TypeError, KeyError, IndexError):
        logger.warning("Dense vector retrieval failed; falling back to BM25 sparse scoring only", exc_info=True)
        elapsed_ms = round((time.perf_counter() - started_wall) * 1000, 3)
        return [], 0.0, elapsed_ms
    else:
        return vector_results, embedding_ms, elapsed_ms


def _merge_dense_results(
    rank_map: dict[str, dict[str, Any]],
    vector_results: list[dict[str, Any]],
    target_category: str | None,
) -> None:
    """Fuse dense ranks into the shared rank map without dropping sparse state."""
    for rank, v_item in enumerate(vector_results, start=1):
        if not _matches_dense_filters(v_item, {}):
            continue
        key = _result_key(v_item, fallback=str(v_item.get("id") or rank))
        if key in rank_map:
            sparse_hash = rank_map[key].get("content_hash")
            dense_hash = v_item.get("content_hash")
            if sparse_hash != dense_hash and (sparse_hash or dense_hash):
                logger.warning("Skipping stale dense hit for %s: sparse/vector content hashes differ", key)
                continue
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
                "content_hash": v_item.get("content_hash"),
                "index_version": v_item.get("index_version"),
                "bm25_rank": None,
                "vector_rank": rank,
                "meta": v_item.get("meta", {}),
            }


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


def _merge_retrieval_filters(extracted_filters: dict[str, Any], explicit_filters: dict[str, Any]) -> dict[str, Any]:
    """Merge query entities without applying unreliable source-specific filters."""
    merged = dict(extracted_filters)
    source_table = explicit_filters.get("source_table")
    if source_table in _SOURCE_FILTERS_WITHOUT_RELIABLE_TEAM_SEASON:
        merged.pop("team_id", None)
        merged.pop("season_year", None)
    merged.update(explicit_filters)
    return merged


class HybridRetriever:
    """RRF (Reciprocal Rank Fusion) Hybrid Retriever for KBO Knowledge Base."""

    def __init__(
        self,
        session: Session,
        k: int = DEFAULT_RRF_K,
        *,
        resolve_entities: bool = True,
        embedding_service: EmbeddingProvider | None = None,
        dense_weight: float = DEFAULT_DENSE_RRF_WEIGHT,
    ) -> None:
        """Initialize retriever.

        Args:
            session: DB Session.
            k: RRF constant parameter (default 2).
            resolve_entities: Resolve canonical player IDs before retrieval.
            embedding_service: Optional dense embedding provider, primarily for deterministic evaluation.
            dense_weight: Weight applied to dense rank contributions during fusion.

        """
        self.session = session
        self.k = k
        self.resolve_entities = resolve_entities
        self.embedding_service = embedding_service
        self.dense_weight = dense_weight
        self.bm25_engine = RagSearchEngine(session)
        self.last_trace: dict[str, Any] = {}

    def retrieve(
        self,
        query: str,
        top_k: int = 5,
        category: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[HybridSearchResult]:
        """Perform hybrid retrieval combining keyword matching and semantic scoring."""
        started = time.perf_counter()
        resolve_started = time.perf_counter()
        if self.resolve_entities:
            resolved = resolve_kbo_entities(self.session, query, filters)
            merged_filters = _merge_retrieval_filters(resolved.to_filters(), filters or {})
        else:
            extracted = extract_kbo_entities(query, extract_player=False)
            merged_filters = _merge_retrieval_filters(extracted.to_filters(), filters or {})
        resolver_ms = round((time.perf_counter() - resolve_started) * 1000, 3)
        target_category = category if category is not None else merged_filters.get("document_type")

        # 1. BM25 Search
        bm25_started = time.perf_counter()
        sparse_top_k = top_k if is_oracle_vector_backend() else top_k * 3
        bm25_chunks = self.bm25_engine.search(
            query=query,
            top_k=sparse_top_k,
            category=target_category,
            filters=merged_filters,
            oracle_ranked_candidates=not is_oracle_vector_backend(),
        )
        bm25_ms = round((time.perf_counter() - bm25_started) * 1000, 3)
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
                "content_hash": item.get("content_hash"),
                "index_version": item.get("index_version"),
                "bm25_rank": rank,
                "vector_rank": None,
                "meta": item.get("meta", {}),
            }

        # 2. Dense Vector Search
        vector_candidates, embedding_ms, vector_ms = _search_dense(
            query,
            top_k,
            merged_filters,
            target_category,
            self.embedding_service,
        )
        _merge_dense_results(rank_map, vector_candidates, target_category)

        # 3. Compute Reciprocal Rank Fusion (RRF) scores
        fusion_started = time.perf_counter()
        results: list[HybridSearchResult] = []
        for key, entry in rank_map.items():
            bm25_r = entry["bm25_rank"]
            vector_r = entry["vector_rank"]

            score = 0.0
            if bm25_r is not None:
                score += BM25_RRF_WEIGHT / (self.k + bm25_r)
            if vector_r is not None:
                score += self.dense_weight / (self.k + vector_r)

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
        fusion_ms = round((time.perf_counter() - fusion_started) * 1000, 3)
        self.last_trace = {
            "mode": "hybrid",
            "route": "DOCUMENT",
            "fallback_used": False,
            "fusion": "RRF",
            "rrf_k": self.k,
            "rerank_candidates": 0,
            "structured_candidates": 0,
            "bm25_candidates": len(bm25_chunks),
            "vector_candidates": vector_candidates,
            "returned": min(len(results), top_k),
            "filters": merged_filters,
            "latency_ms": {
                "router": 0.0,
                "resolver": resolver_ms,
                "structured": 0.0,
                "bm25": bm25_ms,
                "embedding": embedding_ms,
                "vector": vector_ms,
                "fusion": fusion_ms,
                "reranker": 0.0,
                "total": round((time.perf_counter() - started) * 1000, 3),
            },
        }
        return results[:top_k]
