"""Unit tests for RagEvaluationGateway."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from src.rag.base_retriever import BaseRetriever
from src.rag.dto import (
    RagGoldenQuery,
    RetrievalCandidate,
    RetrievalQuery,
    RetrievalResult,
)
from src.rag.evaluation_gateway import RagEvaluationGateway


class DummyRetriever(BaseRetriever):
    """Dummy retriever for testing evaluation gateway."""

    def __init__(self, candidates_map: dict[str, list[str]] | None = None) -> None:
        super().__init__(name="DummyRetriever")
        self.candidates_map = candidates_map or {}

    def retrieve(self, query: RetrievalQuery | str, **kwargs: object) -> RetrievalResult:
        norm_q = self._normalize_query(query, **kwargs)
        matched_chunk_ids = self.candidates_map.get(norm_q.query_text, [])

        candidates = [
            RetrievalCandidate(
                chunk_id=cid,
                title=f"Title {cid}",
                content=f"Content {cid}",
                score=1.0 / (i + 1),
            )
            for i, cid in enumerate(matched_chunk_ids)
        ]
        return RetrievalResult(
            query=norm_q,
            candidates=candidates[: norm_q.top_k],
            elapsed_ms=15.0,
        )


def test_load_golden_queries_from_json(tmp_path: Path) -> None:
    """Test loading golden query JSON format."""
    data = [
        {
            "id": "q1",
            "query": "김도영 타격 기록",
            "relevantChunkIds": ["chunk_1", "chunk_2"],
            "filters": {"season_year": 2026},
            "tags": ["batting"],
        },
        {
            "id": "q2",
            "query": "양현종 투구 성적",
            "relevant_chunk_ids": ["chunk_3"],
        },
    ]
    json_path = tmp_path / "golden.json"
    json_path.write_text(json.dumps(data), encoding="utf-8")

    session = MagicMock(spec=Session)
    gateway = RagEvaluationGateway(session)
    queries = gateway.load_golden_queries(json_path)

    assert len(queries) == 2
    assert queries[0].id == "q1"
    assert queries[0].query == "김도영 타격 기록"
    assert queries[0].relevant_chunk_ids == ["chunk_1", "chunk_2"]
    assert queries[0].filters == {"season_year": 2026}
    assert queries[0].tags == ["batting"]

    assert queries[1].id == "q2"
    assert queries[1].relevant_chunk_ids == ["chunk_3"]


def test_load_golden_queries_file_not_found() -> None:
    """Test loading non-existent golden query file raises FileNotFoundError."""
    session = MagicMock(spec=Session)
    gateway = RagEvaluationGateway(session)
    with pytest.raises(FileNotFoundError):
        gateway.load_golden_queries("non_existent_file.json")


def test_calculate_latency_percentiles() -> None:
    """Test latency percentiles calculation."""
    samples = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    breakdown = RagEvaluationGateway.calculate_latency_percentiles(samples)

    assert breakdown.p50_ms == pytest.approx(55.0, abs=1.0)
    assert breakdown.p90_ms == pytest.approx(91.0, abs=2.0)
    assert breakdown.p95_ms == pytest.approx(95.5, abs=2.0)
    assert breakdown.max_ms == 100.0
    assert breakdown.avg_ms == 55.0


def test_evaluate_golden_set_perfect_match() -> None:
    """Test evaluation report when all targets match on top rank."""
    queries = [
        RagGoldenQuery(id="q1", query="김도영 성적", relevant_chunk_ids=["chunk_1"]),
        RagGoldenQuery(id="q2", query="구자욱 성적", relevant_chunk_ids=["chunk_2"]),
    ]
    retriever = DummyRetriever(
        candidates_map={
            "김도영 성적": ["chunk_1", "other_1"],
            "구자욱 성적": ["chunk_2", "other_2"],
        }
    )

    session = MagicMock(spec=Session)
    gateway = RagEvaluationGateway(session, retriever=retriever)
    report = gateway.evaluate_golden_set(queries, top_k=5, min_recall=0.8, min_mrr=0.7)

    assert report.total_evaluated == 2
    assert report.metrics.recall_at_k == 1.0
    assert report.metrics.mrr == 1.0
    assert report.metrics.hit_rate == 1.0
    assert report.sla_passed is True
    assert len(report.sla_violations) == 0
    assert len(report.details) == 2


def test_evaluate_golden_set_sla_violations() -> None:
    """Test evaluation report properly flags SLA violations."""
    queries = [
        RagGoldenQuery(id="q1", query="미등록 질의", relevant_chunk_ids=["chunk_target"]),
    ]
    retriever = DummyRetriever(
        candidates_map={
            "미등록 질의": ["wrong_chunk_1", "wrong_chunk_2"],
        }
    )

    session = MagicMock(spec=Session)
    gateway = RagEvaluationGateway(session, retriever=retriever)
    report = gateway.evaluate_golden_set(
        queries,
        top_k=5,
        min_recall=0.85,
        min_mrr=0.70,
        max_p95_ms=0.001,  # Intentional latency SLA failure
    )

    assert report.total_evaluated == 1
    assert report.metrics.recall_at_k == 0.0
    assert report.metrics.mrr == 0.0
    assert report.metrics.hit_rate == 0.0
    assert report.sla_passed is False
    assert len(report.sla_violations) >= 2
