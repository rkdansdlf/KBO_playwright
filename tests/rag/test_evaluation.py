"""Unit tests for src.rag.evaluation."""

from __future__ import annotations

from src.rag.evaluation import RagEvaluator


def test_evaluator_precision_and_recall() -> None:
    evaluator = RagEvaluator()
    retrieved = ["doc_1", "doc_2", "doc_3", "doc_4", "doc_5"]
    relevant = ["doc_2", "doc_5", "doc_9"]

    p_at_3 = evaluator.calculate_precision_at_k(retrieved, relevant, k=3)
    assert round(p_at_3, 4) == round(1 / 3, 4)

    p_at_5 = evaluator.calculate_precision_at_k(retrieved, relevant, k=5)
    assert round(p_at_5, 4) == round(2 / 5, 4)

    r_at_5 = evaluator.calculate_recall_at_k(retrieved, relevant, k=5)
    assert round(r_at_5, 4) == round(2 / 3, 4)


def test_evaluator_reciprocal_rank() -> None:
    evaluator = RagEvaluator()
    assert evaluator.calculate_reciprocal_rank(["doc_1", "doc_2", "doc_3"], ["doc_1"]) == 1.0
    assert evaluator.calculate_reciprocal_rank(["doc_1", "doc_2", "doc_3"], ["doc_2"]) == 0.5
    assert round(evaluator.calculate_reciprocal_rank(["doc_1", "doc_2", "doc_3"], ["doc_3"]), 4) == round(1 / 3, 4)
    assert evaluator.calculate_reciprocal_rank(["doc_1", "doc_2", "doc_3"], ["doc_99"]) == 0.0


def test_evaluator_ndcg_at_k() -> None:
    evaluator = RagEvaluator()
    retrieved = ["doc_1", "doc_2", "doc_3"]
    relevant = ["doc_1", "doc_2"]

    # Perfect ranking at k=2 -> NDCG = 1.0
    ndcg = evaluator.calculate_ndcg_at_k(retrieved, relevant, k=2)
    assert round(ndcg, 4) == 1.0


def test_evaluator_batch() -> None:
    evaluator = RagEvaluator()
    batch_ret = [
        ["d1", "d2", "d3"],
        ["d4", "d5", "d6"],
    ]
    batch_rel = [
        ["d1"],
        ["d6"],
    ]

    metrics = evaluator.evaluate_batch(batch_ret, batch_rel, k=3)
    assert metrics.sample_count == 2
    assert metrics.hit_rate == 1.0
    # Q1 RR = 1.0 (d1 at rank 1), Q2 RR = 1/3 (d6 at rank 3) -> MRR = (1 + 1/3) / 2 = 2/3 = 0.6667
    assert round(metrics.mrr, 4) == round(2 / 3, 4)
