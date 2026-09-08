"""Tests for hybrid retrieval degradation and benchmark evaluation."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.models.rag_chunk import RagChunk
from src.rag.dto import RetrievalCandidate, RetrievalQuery, RetrievalResult
from src.rag.evaluation_gateway import OfflineVectorSearchRepository, RagEvaluationGateway
from src.rag.retrievers.hybrid import UnifiedHybridRetriever
from src.services.rag_eval_corpus import DeterministicEmbeddingService


def test_hybrid_retriever_branch_degradation_when_dense_fails() -> None:
    """Test hybrid search gracefully falls back to sparse results when dense branch throws exception."""
    session = MagicMock()
    failing_dense = MagicMock()
    failing_dense.retrieve.side_effect = RuntimeError("Dense vector connection timeout")

    sparse_mock = MagicMock()
    sparse_candidate = RetrievalCandidate(
        chunk_id="eval_rulebook:rule_abs:1",
        title="ABS Rule",
        content="Strike zone rule",
        score=0.9,
    )
    sparse_mock.retrieve.return_value = RetrievalResult(
        query=RetrievalQuery(query_text="ABS"),
        candidates=[sparse_candidate],
        elapsed_ms=5.0,
        retrieval_mode="sparse_bm25",
    )

    retriever = UnifiedHybridRetriever(
        session=session,
        dense_retriever=failing_dense,
        sparse_retriever=sparse_mock,
    )

    result = retriever.retrieve("ABS")
    assert result is not None
    assert len(result.candidates) == 1
    assert result.candidates[0].chunk_id == "eval_rulebook:rule_abs:1"


def test_hybrid_retriever_branch_degradation_when_sparse_fails() -> None:
    """Test hybrid search gracefully falls back to dense results when sparse branch throws exception."""
    session = MagicMock()
    failing_sparse = MagicMock()
    failing_sparse.retrieve.side_effect = OSError("Database unreachable")

    dense_mock = MagicMock()
    dense_candidate = RetrievalCandidate(
        chunk_id="eval_player:player_kim:1",
        title="Kim Do-young",
        content="Player profile",
        score=0.85,
    )
    dense_mock.retrieve.return_value = RetrievalResult(
        query=RetrievalQuery(query_text="김도영"),
        candidates=[dense_candidate],
        elapsed_ms=8.0,
        retrieval_mode="dense_vector",
    )

    retriever = UnifiedHybridRetriever(
        session=session,
        dense_retriever=dense_mock,
        sparse_retriever=failing_sparse,
    )

    result = retriever.retrieve("김도영")
    assert result is not None
    assert len(result.candidates) == 1
    assert result.candidates[0].chunk_id == "eval_player:player_kim:1"


def test_offline_vector_search_repository_ranking() -> None:
    """Test offline vector repository computes unit cosine similarity correctly."""
    embedder = DeterministicEmbeddingService()
    emb_abs = embedder.get_embedding("ABS 스트라이크존 공식 규정")
    emb_walk = embedder.get_embedding("고의사구 규칙")

    chunk1 = RagChunk(
        source_table="eval_rulebook",
        source_row_id="rule_abs",
        title="ABS 규정",
        content="ABS 스트라이크존 공식 규정",
        embedding=emb_abs,
    )
    chunk2 = RagChunk(
        source_table="eval_rulebook",
        source_row_id="rule_walk",
        title="고의사구 규정",
        content="고의사구 규칙",
        embedding=emb_walk,
    )

    repo = OfflineVectorSearchRepository([chunk1, chunk2], embedder)
    query_vector = embedder.get_embedding("ABS 판정 기준")
    results = repo.search_similar(query_vector=query_vector, top_k=2)

    assert len(results) == 2
    assert results[0]["chunk_id"] == "eval_rulebook:rule_abs"
    assert results[0]["score"] >= results[1]["score"]


def test_setup_offline_fixture_gateway_and_evaluate_variants() -> None:
    """Test setting up offline gateway and evaluating multiple retriever variants."""
    gateway, variants = RagEvaluationGateway.setup_offline_fixture_gateway()
    assert "sparse" in variants
    assert "dense" in variants
    assert "hybrid" in variants

    reports = gateway.evaluate_variants(variants, top_k=3, max_queries=5)
    assert len(reports) == 3
    for name in ("sparse", "dense", "hybrid"):
        assert name in reports
        rep = reports[name]
        assert rep.total_evaluated == 5
        assert rep.metrics.recall_at_k >= 0.0
        assert rep.latency.p95_ms > 0.0
