"""Unit tests for src.rag.dto."""

from __future__ import annotations

from src.rag.dto import (
    RagDocument,
    RagEvaluationMetrics,
    RetrievalCandidate,
    RetrievalQuery,
    RetrievalResult,
)


def test_rag_document_to_dict() -> None:
    doc = RagDocument(
        chunk_id="chk_123",
        title="2025 KBO 일정 공시",
        content="2025 신한 SOL Bank KBO 리그 정규시즌 일정 확정",
        category="press_release",
        source_table="kbo_press_release",
        source_row_id="101",
        team_id="LG",
        player_id=None,
        season_year=2025,
        document_type="schedule",
        game_date=None,
        published_at="2025-03-01",
        source_url="https://www.koreabaseball.com/News/Notice/View.aspx?bdSe=101",
        language="ko",
        metadata={"priority": "high"},
    )
    data = doc.to_dict()
    assert data["chunk_id"] == "chk_123"
    assert data["title"] == "2025 KBO 일정 공시"
    assert data["category"] == "press_release"
    assert data["source_table"] == "kbo_press_release"
    assert data["team_id"] == "LG"
    assert data["season_year"] == 2025
    assert data["language"] == "ko"
    assert data["metadata"]["priority"] == "high"


def test_retrieval_query_to_dict() -> None:
    query = RetrievalQuery(
        query_text="김도영 30-30 달성",
        top_k=10,
        category="milestone",
        filters={"season_year": 2024},
        dense_weight=2.5,
        sparse_weight=1.5,
        rrf_k=3,
        language="ko",
        resolve_entities=True,
    )
    data = query.to_dict()
    assert data["query_text"] == "김도영 30-30 달성"
    assert data["top_k"] == 10
    assert data["category"] == "milestone"
    assert data["dense_weight"] == 2.5
    assert data["sparse_weight"] == 1.5
    assert data["rrf_k"] == 3
    assert data["resolve_entities"] is True


def test_retrieval_candidate_and_result_to_dict() -> None:
    cand = RetrievalCandidate(
        chunk_id="chunk_99",
        title="김도영 최연소 30-30 달성",
        content="KIA 타이거즈 김도영 선수가 30홈런 30도루를 달성했습니다.",
        score=0.88421,
        category="milestone",
        source_url="https://koreabaseball.com/milestones/99",
        vector_rank=1,
        bm25_rank=2,
        metadata={"player_name": "김도영"},
        provenance={"source": "kbo_official"},
    )
    cand_dict = cand.to_dict()
    assert cand_dict["chunk_id"] == "chunk_99"
    assert cand_dict["score"] == 0.8842
    assert cand_dict["vector_rank"] == 1
    assert cand_dict["bm25_rank"] == 2
    assert cand_dict["metadata"]["player_name"] == "김도영"

    query = RetrievalQuery(query_text="김도영 홈런")
    res = RetrievalResult(
        query=query,
        candidates=[cand],
        elapsed_ms=12.345,
        retrieval_mode="hybrid_rrf",
        total_matches=1,
        resolved_entities={"player_id": "54609", "player_name": "김도영"},
    )
    res_dict = res.to_dict()
    assert res_dict["elapsed_ms"] == 12.35
    assert res_dict["retrieval_mode"] == "hybrid_rrf"
    assert len(res_dict["candidates"]) == 1
    assert res_dict["resolved_entities"]["player_name"] == "김도영"


def test_rag_evaluation_metrics_to_dict() -> None:
    metrics = RagEvaluationMetrics(
        precision_at_k=0.8,
        recall_at_k=0.6,
        mrr=0.91234,
        ndcg=0.87654,
        hit_rate=1.0,
        sample_count=20,
    )
    data = metrics.to_dict()
    assert data["precision_at_k"] == 0.8
    assert data["recall_at_k"] == 0.6
    assert data["mrr"] == 0.9123
    assert data["ndcg"] == 0.8765
    assert data["hit_rate"] == 1.0
    assert data["sample_count"] == 20
