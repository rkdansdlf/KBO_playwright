"""Integration tests for grounded corpus benchmark execution."""

from __future__ import annotations

from pathlib import Path

from src.rag.benchmark_runner import RagBenchmarkRunner
from src.rag.dto import BenchmarkVariantConfig

CORPUS_PATH = Path("Docs/references/rag_benchmark_corpus_documents.json")
DATASET_PATH = Path("Docs/references/rag_golden_benchmark_dataset.json")


def test_grounded_runner_build_and_evaluation() -> None:
    """Test that build_grounded_runner populates in-memory chunks and yields high recall."""
    assert CORPUS_PATH.exists(), f"Missing corpus file: {CORPUS_PATH}"
    assert DATASET_PATH.exists(), f"Missing dataset file: {DATASET_PATH}"

    runner = RagBenchmarkRunner.build_grounded_runner(CORPUS_PATH)
    queries = runner.load_dataset(DATASET_PATH)[:10]  # First 10 factoid queries

    variants = [
        BenchmarkVariantConfig(
            name="sparse_bm25",
            mode="sparse_bm25",
            description="Sparse BM25",
        ),
        BenchmarkVariantConfig(
            name="hybrid_rrf_default",
            mode="hybrid",
            rrf_k=2,
            dense_weight=2.0,
            sparse_weight=1.0,
            description="Default RRF Hybrid",
        ),
    ]

    report = runner.run_benchmark(queries, variants=variants, top_k=5)
    assert report.total_queries == 10
    assert "sparse_bm25" in report.variants
    assert "hybrid_rrf_default" in report.variants

    sparse_metrics = report.variants["sparse_bm25"].metrics
    hybrid_metrics = report.variants["hybrid_rrf_default"].metrics

    # Grounded benchmark must achieve high recall and hit rate
    assert sparse_metrics.hit_rate >= 0.70
    assert hybrid_metrics.hit_rate >= 0.80
    assert hybrid_metrics.recall_at_k >= 0.80
