"""Unit tests for RAG Generation and Groundedness Evaluation."""

from __future__ import annotations

from src.rag.dto import QnaSample
from src.rag.generation_evaluation import RagGenerationEvaluator


def test_grounded_answer_passes() -> None:
    """Test that a fully grounded factual answer scores high groundedness and 0 hallucinations."""
    sample = QnaSample(
        query="2024년 김도영 선수의 홈런과 도루 기록은?",
        answer="2024시즌 김도영 선수는 38홈런과 40도루를 기록하며 KBO 최초 월간 10-10 및 30-30 클럽에 가입했습니다.",
        context_chunks=[
            "2024시즌 KIA 타이거즈 김도영 선수의 공식 기록: 38홈런, 40도루. KBO 역대 최연소 30-30 달성.",
            "김도영은 2024년 4월 KBO 최초 월간 10-10을 기록했습니다.",
        ],
        target_entities={"player_name": "김도영", "team_id": "KIA", "season_year": 2024},
        expected_keywords=["38홈런", "40도루", "30-30"],
        citations=["2024시즌 KIA 타이거즈 김도영 선수의 공식 기록"],
    )

    metrics = RagGenerationEvaluator.evaluate_sample(sample)
    assert metrics.groundedness_score >= 0.85
    assert metrics.hallucination_rate == 0.0
    assert not metrics.hallucinated_tokens
    assert metrics.answer_relevance >= 0.80
    assert metrics.citation_precision == 1.0


def test_hallucinated_stats_detected() -> None:
    """Test that fake numbers or ungrounded claims are detected as hallucinations."""
    sample = QnaSample(
        query="2024년 김도영 선수의 홈런 기록은?",
        answer="김도영 선수는 2024시즌에 50홈런과 60도루를 기록했습니다.",
        context_chunks=[
            "2024시즌 KIA 타이거즈 김도영 선수의 공식 기록: 38홈런, 40도루.",
        ],
        target_entities={"player_name": "김도영", "season_year": 2024},
    )

    metrics = RagGenerationEvaluator.evaluate_sample(sample)
    assert metrics.hallucination_rate > 0.0
    assert "50홈런" in metrics.hallucinated_tokens or "50" in " ".join(metrics.hallucinated_tokens)
    assert "60도루" in metrics.hallucinated_tokens or "60" in " ".join(metrics.hallucinated_tokens)


def test_irrelevant_answer_low_relevance() -> None:
    """Test that an off-topic answer yields lower relevance."""
    sample = QnaSample(
        query="양현종 선수의 통산 탈삼진 수는?",
        answer="잠실야구장 3루 테이블석 가격은 주말 기준 50000원입니다.",
        context_chunks=[
            "양현종 선수는 통산 2000탈삼진을 돌파한 베테랑 좌완 투수입니다.",
        ],
        target_entities={"player_name": "양현종"},
    )

    metrics = RagGenerationEvaluator.evaluate_sample(sample)
    assert metrics.answer_relevance < 0.50


def test_evaluate_batch_and_render_markdown() -> None:
    """Test batch evaluation and scorecard rendering."""
    samples = [
        QnaSample(
            query="김도영 30-30 달성",
            answer="김도영은 2024시즌 38홈런 40도루로 30-30을 달성했습니다.",
            context_chunks=["2024시즌 김도영 38홈런 40도루 달성. 30-30 클럽 가입."],
        ),
        QnaSample(
            query="ABS 규정",
            answer="ABS는 자동 투구 판정 시스템입니다.",
            context_chunks=["KBO는 2024시즌부터 자동 투구 판정 시스템(ABS)을 전면 도입했습니다."],
        ),
    ]

    report = RagGenerationEvaluator.evaluate_batch(samples)
    assert report.total_samples == 2
    assert report.avg_groundedness >= 0.85
    assert report.avg_hallucination_rate <= 0.05

    markdown = RagGenerationEvaluator.render_markdown_report(report)
    assert "# 📝 KBO RAG Generation & Groundedness Scorecard" in markdown
    assert "Groundedness Score" in markdown
