"""Standard Data Transfer Objects (DTOs) for the KBO RAG Pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RagDocument:
    """Represents a knowledge chunk/document in the KBO RAG system."""

    chunk_id: str
    title: str | None
    content: str
    category: str = "general"
    source_table: str | None = None
    source_row_id: str | None = None
    team_id: str | None = None
    player_id: str | None = None
    season_year: int | None = None
    document_type: str | None = None
    game_date: str | None = None
    published_at: str | None = None
    source_url: str | None = None
    language: str = "ko"
    metadata: dict[str, Any] = field(default_factory=dict)
    embedding: list[float] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert document to a serializable dictionary."""
        return {
            "chunk_id": self.chunk_id,
            "title": self.title,
            "content": self.content,
            "category": self.category,
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
            "metadata": self.metadata,
        }


@dataclass
class RetrievalQuery:
    """Represents a structured search request for the retriever."""

    query_text: str
    top_k: int = 5
    category: str | None = None
    filters: dict[str, Any] | None = None
    dense_weight: float = 2.0
    sparse_weight: float = 1.0
    rrf_k: int = 2
    language: str = "ko"
    resolve_entities: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert query to dictionary."""
        return {
            "query_text": self.query_text,
            "top_k": self.top_k,
            "category": self.category,
            "filters": self.filters,
            "dense_weight": self.dense_weight,
            "sparse_weight": self.sparse_weight,
            "rrf_k": self.rrf_k,
            "language": self.language,
            "resolve_entities": self.resolve_entities,
            "metadata": self.metadata,
        }


@dataclass
class RetrievalCandidate:
    """Represents a single retrieved document item with scoring and rank metadata."""

    chunk_id: str
    title: str | None
    content: str
    score: float
    category: str = "general"
    source_url: str | None = None
    vector_rank: int | None = None
    bm25_rank: int | None = None
    source_table: str | None = None
    source_row_id: str | None = None
    team_id: str | None = None
    player_id: str | None = None
    season_year: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert candidate to a serializable dictionary."""
        return {
            "chunk_id": self.chunk_id,
            "title": self.title,
            "content": self.content,
            "score": round(self.score, 4),
            "category": self.category,
            "source_url": self.source_url,
            "vector_rank": self.vector_rank,
            "bm25_rank": self.bm25_rank,
            "source_table": self.source_table,
            "source_row_id": self.source_row_id,
            "team_id": self.team_id,
            "player_id": self.player_id,
            "season_year": self.season_year,
            "metadata": self.metadata,
            "provenance": self.provenance,
        }


@dataclass
class RetrievalResult:
    """Represents the complete result of a retrieval operation."""

    query: RetrievalQuery
    candidates: list[RetrievalCandidate] = field(default_factory=list)
    elapsed_ms: float = 0.0
    retrieval_mode: str = "hybrid"
    total_matches: int = 0
    resolved_entities: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert retrieval result to a serializable dictionary."""
        return {
            "query": self.query.to_dict(),
            "candidates": [c.to_dict() for c in self.candidates],
            "elapsed_ms": round(self.elapsed_ms, 2),
            "retrieval_mode": self.retrieval_mode,
            "total_matches": self.total_matches or len(self.candidates),
            "resolved_entities": self.resolved_entities,
        }


@dataclass
class RagEvaluationMetrics:
    """Metrics for evaluating RAG retrieval quality."""

    precision_at_k: float = 0.0
    recall_at_k: float = 0.0
    mrr: float = 0.0
    ndcg: float = 0.0
    hit_rate: float = 0.0
    sample_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert evaluation metrics to dictionary."""
        return {
            "precision_at_k": round(self.precision_at_k, 4),
            "recall_at_k": round(self.recall_at_k, 4),
            "mrr": round(self.mrr, 4),
            "ndcg": round(self.ndcg, 4),
            "hit_rate": round(self.hit_rate, 4),
            "sample_count": self.sample_count,
        }
