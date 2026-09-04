"""Unit tests for RagBenchmarkRunner and KBO hybrid search ablation analysis."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from src.rag.base_retriever import BaseRetriever
from src.rag.benchmark_runner import RagBenchmarkRunner, get_default_variant_configs
from src.rag.dto import (
    BenchmarkVariantConfig,
    RagGoldenQuery,
    RetrievalCandidate,
    RetrievalQuery,
    RetrievalResult,
)


class MockBenchmarkRetriever(BaseRetriever):
    """Mock retriever that simulates dense and sparse results with controlled ranking."""

    def __init__(self, mode: str = "hybrid") -> None:
        super().__init__(name=f"MockRetriever_{mode}")
        self.mode = mode

    def retrieve(self, query: RetrievalQuery | str, **kwargs: object) -> RetrievalResult:
        norm_q = self._normalize_query(query, **kwargs)
        text = norm_q.query_text

        # Generate candidates based on query text
        if "김도영" in text:
            target_cid = "player_season_batting:52605_2026_KIA_REGULAR"
            other_cid = "player_season_batting:other"
            target_cand = RetrievalCandidate(
                chunk_id=target_cid,
                title="김도영 타격 성적",
                content="2026년 김도영 3할 30홈런 달성",
                score=0.95,
                team_id="KIA",
                player_id="52605",
                season_year=2026,
            )
            other_cand = RetrievalCandidate(
                chunk_id=other_cid,
                title="기타 타자 성적",
                content="2026년 일반 타자 기록",
                score=0.40,
                team_id="SS",
                season_year=2026,
            )
            cands = [target_cand, other_cand]
        elif "양현종" in text or "대투수" in text:
            target_cid = "player_season_pitching:3632_2026_KIA_REGULAR"
            target_cand = RetrievalCandidate(
                chunk_id=target_cid,
                title="양현종 투구 성적",
                content="2026년 양현종 평균자책점 3.15",
                score=0.92,
                team_id="KIA",
                player_id="3632",
                season_year=2026,
            )
            cands = [target_cand]
        else:
            cands = [
                RetrievalCandidate(
                    chunk_id="generic_doc_1",
                    title="야구 일반 지식",
                    content="KBO 리그 일반 규정 및 통계",
                    score=0.5,
                )
            ]

        # In BM25 only mode, simulate lower score for slang queries
        if self.mode == "sparse_bm25" and "대투수" in text:
            cands = []

        return RetrievalResult(
            query=norm_q,
            candidates=cands[: norm_q.top_k],
            elapsed_ms=12.5,
            retrieval_mode=self.mode,
        )


@pytest.fixture
def sample_benchmark_queries() -> list[RagGoldenQuery]:
    return [
        RagGoldenQuery(
            id="q1",
            query="김도영의 2026시즌 타율과 홈런",
            category="factoid_stat",
            difficulty="easy",
            relevant_chunk_ids=["player_season_batting:52605_2026_KIA_REGULAR"],
            target_entities={"player_name": "김도영", "player_id": "52605", "team_id": "KIA", "season_year": 2026},
            expected_keywords=["김도영", "30홈런"],
        ),
        RagGoldenQuery(
            id="q2",
            query="대투수 양현종 올해 방어율",
            category="slang_fuzzy_alias",
            difficulty="hard",
            relevant_chunk_ids=["player_season_pitching:3632_2026_KIA_REGULAR"],
            target_entities={"player_name": "양현종", "player_id": "3632", "team_id": "KIA", "season_year": 2026},
            expected_keywords=["양현종", "평균자책점"],
        ),
    ]


def test_default_variant_configs() -> None:
    configs = get_default_variant_configs()
    assert len(configs) >= 4
    names = [c.name for c in configs]
    assert "sparse_bm25" in names
    assert "dense_vector" in names
    assert "hybrid_rrf_default" in names
    assert "resolver_hybrid" in names


def test_runner_evaluate_variant(sample_benchmark_queries: list[RagGoldenQuery]) -> None:
    session = MagicMock(spec=Session)
    mock_dense = MockBenchmarkRetriever(mode="dense_vector")
    mock_sparse = MockBenchmarkRetriever(mode="sparse_bm25")

    runner = RagBenchmarkRunner(
        session,
        dense_retriever=mock_dense,  # type: ignore[arg-type]
        sparse_retriever=mock_sparse,  # type: ignore[arg-type]
    )

    var_cfg = BenchmarkVariantConfig(
        name="test_hybrid",
        mode="hybrid_rrf",
        rrf_k=2,
        dense_weight=2.0,
        sparse_weight=1.0,
        resolve_entities=True,
    )

    report = runner.evaluate_variant(var_cfg, sample_benchmark_queries, top_k=5)

    assert report.total_evaluated == 2
    assert report.top_k == 5
    assert report.metrics.recall_at_k == 1.0
    assert report.metrics.mrr == 1.0
    assert report.metrics.hit_rate == 1.0
    assert report.metrics.entity_match_rate == 0.75
    assert report.metrics.entity_hit_at_1 == 1.0
    assert report.metrics.entity_purity_at_k == 0.75
    assert report.metrics.temporal_fidelity_rate == 1.0
    assert report.metrics.containment_rate == 0.75
    assert report.latency.avg_ms > 0.0
    assert len(report.details) == 2

    # Test concurrent execution
    conc_report = runner.evaluate_variant(var_cfg, sample_benchmark_queries, top_k=5, concurrency=2)
    assert conc_report.total_evaluated == 2
    assert conc_report.metrics.entity_hit_at_1 == 1.0
    assert conc_report.metrics.entity_purity_at_k == 0.75


def test_runner_run_benchmark_and_scorecard(
    sample_benchmark_queries: list[RagGoldenQuery],
    tmp_path: Path,
) -> None:
    session = MagicMock(spec=Session)
    mock_dense = MockBenchmarkRetriever(mode="dense_vector")
    mock_sparse = MockBenchmarkRetriever(mode="sparse_bm25")

    runner = RagBenchmarkRunner(
        session,
        dense_retriever=mock_dense,  # type: ignore[arg-type]
        sparse_retriever=mock_sparse,  # type: ignore[arg-type]
    )

    variants = [
        BenchmarkVariantConfig(name="sparse_bm25", mode="sparse_bm25"),
        BenchmarkVariantConfig(name="dense_vector", mode="dense_vector"),
        BenchmarkVariantConfig(name="hybrid_rrf_default", mode="hybrid_rrf", rrf_k=2, dense_weight=2.0),
    ]

    bench_report = runner.run_benchmark(sample_benchmark_queries, variants=variants, top_k=5)

    assert bench_report.total_queries == 2
    assert len(bench_report.variants) == 3
    assert "factoid_stat" in bench_report.category_breakdown
    assert "slang_fuzzy_alias" in bench_report.category_breakdown
    assert bench_report.best_variant != ""

    # Test Markdown rendering
    scorecard_md = runner.render_markdown_scorecard(bench_report)
    assert "# ⚾ KBO RAG & Hybrid Retrieval Benchmark Scorecard" in scorecard_md
    assert "Ablation Variants Comparison" in scorecard_md
    assert "sparse_bm25" in scorecard_md
    assert "hybrid_rrf_default" in scorecard_md

    # Test export
    out_file = tmp_path / "benchmark_test_report.json"
    runner.export_report(bench_report, out_file)
    assert out_file.exists()
    assert (tmp_path / "benchmark_test_report.md").exists()

    loaded_json = json.loads(out_file.read_text(encoding="utf-8"))
    assert loaded_json["total_queries"] == 2
    assert "variants" in loaded_json


def test_runner_grid_search(sample_benchmark_queries: list[RagGoldenQuery]) -> None:
    session = MagicMock(spec=Session)
    mock_dense = MockBenchmarkRetriever(mode="dense_vector")
    mock_sparse = MockBenchmarkRetriever(mode="sparse_bm25")

    runner = RagBenchmarkRunner(
        session,
        dense_retriever=mock_dense,  # type: ignore[arg-type]
        sparse_retriever=mock_sparse,  # type: ignore[arg-type]
    )

    grid_results = runner.run_grid_search(
        sample_benchmark_queries,
        k_values=(2, 60),
        dense_weights=(1.0, 2.0),
        sparse_weights=(1.0,),
        top_k=5,
    )

    assert len(grid_results) == 4  # 2 k_values * 2 dense_weights
    top_config = grid_results[0]
    assert "rrf_k" in top_config
    assert "mrr" in top_config
    assert "recall_at_k" in top_config
