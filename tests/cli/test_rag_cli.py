"""Tests for RAG CLI commands and Master CLI router integration."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.cli.kbo import main as kbo_main
from src.cli.rag.evaluate import main as eval_main
from src.cli.rag.query import main as query_main
from src.rag.dto import (
    RagEvaluationMetrics,
    RagEvaluationReport,
    RagLatencyBreakdown,
    RetrievalCandidate,
    RetrievalQuery,
    RetrievalResult,
)


def test_rag_query_cli_markdown(capsys) -> None:
    """Test rag query CLI formatted output."""
    mock_result = RetrievalResult(
        query=RetrievalQuery(query_text="김도영 성적"),
        candidates=[
            RetrievalCandidate(
                chunk_id="chunk_123",
                title="김도영 2026 프로필",
                content="김도영 선수는 2026시즌 3할 30홈런을 기록했습니다.",
                score=0.95,
                source_url="https://kbo.co.kr/player/52605",
            )
        ],
        elapsed_ms=12.5,
    )

    with (
        patch("src.cli.rag.query.UnifiedHybridRetriever") as mock_retriever_cls,
        patch("src.cli.rag.query.get_db_session"),
    ):
        mock_instance = MagicMock()
        mock_instance.retrieve.return_value = mock_result
        mock_retriever_cls.return_value = mock_instance

        exit_code = query_main(["김도영 성적"])
        assert exit_code == 0

        captured = capsys.readouterr().out
        assert "김도영 성적" in captured
        assert "김도영 2026 프로필" in captured
        assert "3할 30홈런" in captured


def test_rag_query_cli_json(capsys) -> None:
    """Test rag query CLI JSON output."""
    mock_result = RetrievalResult(
        query=RetrievalQuery(query_text="양현종"),
        candidates=[
            RetrievalCandidate(
                chunk_id="chunk_yang",
                title="양현종 기록",
                content="양현종 투수 기록",
                score=0.88,
            )
        ],
        elapsed_ms=8.0,
    )

    with (
        patch("src.cli.rag.query.UnifiedHybridRetriever") as mock_retriever_cls,
        patch("src.cli.rag.query.get_db_session"),
    ):
        mock_instance = MagicMock()
        mock_instance.retrieve.return_value = mock_result
        mock_retriever_cls.return_value = mock_instance

        exit_code = query_main(["양현종", "--json"])
        assert exit_code == 0

        captured = capsys.readouterr().out
        parsed = json.loads(captured)
        assert parsed["query"]["query_text"] == "양현종"
        assert len(parsed["candidates"]) == 1
        assert parsed["candidates"][0]["chunk_id"] == "chunk_yang"


def test_rag_evaluate_cli_success(capsys) -> None:
    """Test rag evaluate CLI success report."""
    mock_report = RagEvaluationReport(
        metrics=RagEvaluationMetrics(
            precision_at_k=0.90,
            recall_at_k=0.92,
            mrr=0.85,
            ndcg=0.88,
            hit_rate=1.0,
            sample_count=10,
        ),
        latency=RagLatencyBreakdown(
            p50_ms=45.0,
            p90_ms=90.0,
            p95_ms=120.0,
            p99_ms=150.0,
            max_ms=180.0,
            avg_ms=60.0,
        ),
        total_evaluated=10,
        top_k=5,
        sla_passed=True,
        sla_violations=[],
        details=[],
    )

    with (
        patch("src.cli.rag.evaluate.RagEvaluationGateway") as mock_gateway_cls,
        patch("src.cli.rag.evaluate.get_db_session"),
    ):
        mock_instance = MagicMock()
        mock_instance.evaluate_golden_set.return_value = mock_report
        mock_gateway_cls.return_value = mock_instance

        exit_code = eval_main(["--strict"])
        assert exit_code == 0

        captured = capsys.readouterr().out
        assert "KBO RAG 검색 품질 & 성능 벤치마크 리포트" in captured
        assert "92.00%" in captured
        assert "통과 (PASSED)" in captured


def test_rag_evaluate_cli_strict_failure(capsys) -> None:
    """Test rag evaluate CLI strict failure on SLA violation."""
    mock_report = RagEvaluationReport(
        metrics=RagEvaluationMetrics(
            recall_at_k=0.50,
            mrr=0.40,
        ),
        latency=RagLatencyBreakdown(
            p95_ms=600.0,
        ),
        total_evaluated=5,
        top_k=5,
        sla_passed=False,
        sla_violations=["Recall@5 below target"],
        details=[],
    )

    with (
        patch("src.cli.rag.evaluate.RagEvaluationGateway") as mock_gateway_cls,
        patch("src.cli.rag.evaluate.get_db_session"),
    ):
        mock_instance = MagicMock()
        mock_instance.evaluate_golden_set.return_value = mock_report
        mock_gateway_cls.return_value = mock_instance

        exit_code = eval_main(["--strict"])
        assert exit_code == 1

        captured = capsys.readouterr().out
        assert "위반 (FAILED)" in captured
        assert "Recall@5 below target" in captured


def test_kbo_master_cli_rag_dispatch(capsys) -> None:
    """Test master CLI routing kbo rag query and evaluate."""
    with (
        patch("src.cli.rag.query.main", return_value=0) as mock_q,
        patch("src.cli.rag.evaluate.main", return_value=0) as mock_eval,
    ):
        res_q = kbo_main(["rag", "query", "KIA 김도영"])
        assert res_q == 0
        mock_q.assert_called_once_with(["KIA 김도영"])

        res_eval = kbo_main(["rag", "evaluate", "--strict"])
        assert res_eval == 0
        mock_eval.assert_called_once_with(["--strict"])
