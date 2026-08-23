"""Evaluation metrics and tooling for KBO RAG retrieval performance."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from src.rag.dto import RagEvaluationMetrics

if TYPE_CHECKING:
    from collections.abc import Sequence


class RagEvaluator:
    """Evaluates RAG retrieval ranking accuracy against ground truth targets."""

    @staticmethod
    def calculate_precision_at_k(retrieved_ids: Sequence[str], relevant_ids: Sequence[str], k: int) -> float:
        """Calculate Precision@K."""
        if k <= 0 or not retrieved_ids or not relevant_ids:
            return 0.0
        top_k = retrieved_ids[:k]
        rel_set = set(relevant_ids)
        hits = sum(1 for cid in top_k if cid in rel_set)
        return hits / float(k)

    @staticmethod
    def calculate_recall_at_k(retrieved_ids: Sequence[str], relevant_ids: Sequence[str], k: int) -> float:
        """Calculate Recall@K."""
        if not relevant_ids or k <= 0 or not retrieved_ids:
            return 0.0
        top_k = retrieved_ids[:k]
        rel_set = set(relevant_ids)
        hits = sum(1 for cid in top_k if cid in rel_set)
        return hits / float(len(rel_set))

    @staticmethod
    def calculate_reciprocal_rank(retrieved_ids: Sequence[str], relevant_ids: Sequence[str]) -> float:
        """Calculate Reciprocal Rank (RR) of the first relevant document."""
        if not relevant_ids or not retrieved_ids:
            return 0.0
        rel_set = set(relevant_ids)
        for rank, cid in enumerate(retrieved_ids, start=1):
            if cid in rel_set:
                return 1.0 / float(rank)
        return 0.0

    @staticmethod
    def calculate_ndcg_at_k(retrieved_ids: Sequence[str], relevant_ids: Sequence[str], k: int) -> float:
        """Calculate Normalized Discounted Cumulative Gain (NDCG@K) with binary relevance."""
        if k <= 0 or not retrieved_ids or not relevant_ids:
            return 0.0
        top_k = retrieved_ids[:k]
        rel_set = set(relevant_ids)

        dcg = 0.0
        for rank, cid in enumerate(top_k, start=1):
            if cid in rel_set:
                dcg += 1.0 / math.log2(rank + 1)

        # Ideal DCG
        ideal_hits = min(k, len(rel_set))
        if ideal_hits == 0:
            return 0.0
        idcg = sum(1.0 / math.log2(r + 1) for r in range(1, ideal_hits + 1))

        return dcg / idcg if idcg > 0 else 0.0

    @classmethod
    def evaluate_query(
        cls,
        retrieved_ids: Sequence[str],
        relevant_ids: Sequence[str],
        *,
        k: int = 5,
    ) -> RagEvaluationMetrics:
        """Evaluate a single query's retrieved items against relevant targets."""
        p_at_k = cls.calculate_precision_at_k(retrieved_ids, relevant_ids, k)
        r_at_k = cls.calculate_recall_at_k(retrieved_ids, relevant_ids, k)
        rr = cls.calculate_reciprocal_rank(retrieved_ids, relevant_ids)
        ndcg = cls.calculate_ndcg_at_k(retrieved_ids, relevant_ids, k)
        hit = 1.0 if any(cid in set(relevant_ids) for cid in retrieved_ids[:k]) else 0.0

        return RagEvaluationMetrics(
            precision_at_k=p_at_k,
            recall_at_k=r_at_k,
            mrr=rr,
            ndcg=ndcg,
            hit_rate=hit,
            sample_count=1,
        )

    @classmethod
    def evaluate_batch(
        cls,
        batch_retrieved: Sequence[Sequence[str]],
        batch_relevant: Sequence[Sequence[str]],
        *,
        k: int = 5,
    ) -> RagEvaluationMetrics:
        """Evaluate a batch of queries and return aggregated mean metrics."""
        if not batch_retrieved or not batch_relevant or len(batch_retrieved) != len(batch_relevant):
            return RagEvaluationMetrics()

        n = len(batch_retrieved)
        sum_p = 0.0
        sum_r = 0.0
        sum_rr = 0.0
        sum_ndcg = 0.0
        sum_hits = 0.0

        for ret, rel in zip(batch_retrieved, batch_relevant, strict=True):
            m = cls.evaluate_query(ret, rel, k=k)
            sum_p += m.precision_at_k
            sum_r += m.recall_at_k
            sum_rr += m.mrr
            sum_ndcg += m.ndcg
            sum_hits += m.hit_rate

        return RagEvaluationMetrics(
            precision_at_k=sum_p / n,
            recall_at_k=sum_r / n,
            mrr=sum_rr / n,
            ndcg=sum_ndcg / n,
            hit_rate=sum_hits / n,
            sample_count=n,
        )
