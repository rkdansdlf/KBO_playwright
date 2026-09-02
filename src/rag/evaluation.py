"""Evaluation metrics and tooling for KBO RAG retrieval performance."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

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
    def calculate_entity_match_rate(
        cls,
        candidates: Sequence[Any],
        target_entities: dict[str, Any] | None,
        k: int = 3,
    ) -> float:
        """Calculate entity match rate in top-K candidates."""
        if not target_entities or not candidates or k <= 0:
            return 1.0  # vacuously true if no entity requirement
        top_k_candidates = candidates[:k]
        if not top_k_candidates:
            return 0.0

        matches = 0
        target_team = str(target_entities.get("team_id") or "").upper()
        target_player = str(target_entities.get("player_id") or "")
        target_name = str(target_entities.get("player_name") or "")

        for cand in top_k_candidates:
            team_id = getattr(cand, "team_id", None) or (cand.get("team_id") if isinstance(cand, dict) else None)
            player_id = getattr(cand, "player_id", None) or (cand.get("player_id") if isinstance(cand, dict) else None)
            content = getattr(cand, "content", "") or (cand.get("content", "") if isinstance(cand, dict) else "")
            title = getattr(cand, "title", "") or (cand.get("title", "") if isinstance(cand, dict) else "")
            text = f"{title} {content}"

            cand_matched = True
            if target_team and team_id and str(team_id).upper() != target_team:
                cand_matched = False
            if target_player and player_id and str(player_id) != target_player:
                cand_matched = False
            if target_name and target_name not in text and player_id is None and team_id is None:
                cand_matched = False
            if cand_matched:
                matches += 1

        return matches / float(len(top_k_candidates))

    @classmethod
    def calculate_temporal_fidelity(
        cls,
        candidates: Sequence[Any],
        target_season: int | None,
        k: int = 3,
    ) -> float:
        """Calculate temporal fidelity (season match rate) in top-K candidates."""
        if target_season is None or not candidates or k <= 0:
            return 1.0  # no temporal constraint
        top_k_candidates = candidates[:k]
        if not top_k_candidates:
            return 0.0

        matches = 0
        for cand in top_k_candidates:
            season = getattr(cand, "season_year", None) or (cand.get("season_year") if isinstance(cand, dict) else None)
            if season is None or season == target_season:
                matches += 1

        return matches / float(len(top_k_candidates))

    @classmethod
    def calculate_containment_rate(
        cls,
        candidates: Sequence[Any],
        expected_keywords: Sequence[str] | None,
        k: int = 3,
    ) -> float:
        """Calculate exact keyword containment in top-K candidate texts."""
        if not expected_keywords or not candidates or k <= 0:
            return 1.0
        top_k_candidates = candidates[:k]
        if not top_k_candidates:
            return 0.0

        matches = 0
        for cand in top_k_candidates:
            content = getattr(cand, "content", "") or (cand.get("content", "") if isinstance(cand, dict) else "")
            title = getattr(cand, "title", "") or (cand.get("title", "") if isinstance(cand, dict) else "")
            full_text = f"{title} {content}"
            if any(kw in full_text for kw in expected_keywords):
                matches += 1

        return matches / float(len(top_k_candidates))

    @classmethod
    def evaluate_query(  # noqa: PLR0913
        cls,
        retrieved_ids: Sequence[str],
        relevant_ids: Sequence[str],
        *,
        k: int = 5,
        candidates: Sequence[Any] | None = None,
        target_entities: dict[str, Any] | None = None,
        target_season: int | None = None,
        expected_keywords: Sequence[str] | None = None,
    ) -> RagEvaluationMetrics:
        """Evaluate a single query's retrieved items against relevant targets."""
        p_at_k = cls.calculate_precision_at_k(retrieved_ids, relevant_ids, k)
        r_at_k = cls.calculate_recall_at_k(retrieved_ids, relevant_ids, k)
        r_at_1 = cls.calculate_recall_at_k(retrieved_ids, relevant_ids, 1)
        r_at_3 = cls.calculate_recall_at_k(retrieved_ids, relevant_ids, 3)
        rr = cls.calculate_reciprocal_rank(retrieved_ids, relevant_ids)
        ndcg = cls.calculate_ndcg_at_k(retrieved_ids, relevant_ids, k)
        ndcg_10 = cls.calculate_ndcg_at_k(retrieved_ids, relevant_ids, 10)
        hit = 1.0 if any(cid in set(relevant_ids) for cid in retrieved_ids[:k]) else 0.0

        entity_match = (
            cls.calculate_entity_match_rate(candidates, target_entities, k=min(k, 3)) if candidates is not None else 1.0
        )
        temporal_fidelity = (
            cls.calculate_temporal_fidelity(candidates, target_season, k=min(k, 3)) if candidates is not None else 1.0
        )
        containment = (
            cls.calculate_containment_rate(candidates, expected_keywords, k=min(k, 3))
            if candidates is not None
            else 1.0
        )

        return RagEvaluationMetrics(
            precision_at_k=p_at_k,
            recall_at_k=r_at_k,
            recall_at_1=r_at_1,
            recall_at_3=r_at_3,
            mrr=rr,
            ndcg=ndcg,
            ndcg_at_10=ndcg_10,
            hit_rate=hit,
            entity_match_rate=entity_match,
            temporal_fidelity_rate=temporal_fidelity,
            containment_rate=containment,
            sample_count=1,
        )

    @classmethod
    def evaluate_batch(  # noqa: PLR0913
        cls,
        batch_retrieved: Sequence[Sequence[str]],
        batch_relevant: Sequence[Sequence[str]],
        *,
        k: int = 5,
        batch_candidates: Sequence[Sequence[Any]] | None = None,
        batch_target_entities: Sequence[dict[str, Any] | None] | None = None,
        batch_target_seasons: Sequence[int | None] | None = None,
        batch_expected_keywords: Sequence[Sequence[str] | None] | None = None,
    ) -> RagEvaluationMetrics:
        """Evaluate a batch of queries and return aggregated mean metrics."""
        if not batch_retrieved or not batch_relevant or len(batch_retrieved) != len(batch_relevant):
            return RagEvaluationMetrics()

        n = len(batch_retrieved)
        sum_p = 0.0
        sum_r = 0.0
        sum_r1 = 0.0
        sum_r3 = 0.0
        sum_rr = 0.0
        sum_ndcg = 0.0
        sum_ndcg10 = 0.0
        sum_hits = 0.0
        sum_entity = 0.0
        sum_temporal = 0.0
        sum_containment = 0.0

        for i, (ret, rel) in enumerate(zip(batch_retrieved, batch_relevant, strict=True)):
            cands = batch_candidates[i] if batch_candidates and i < len(batch_candidates) else None
            entities = batch_target_entities[i] if batch_target_entities and i < len(batch_target_entities) else None
            season = batch_target_seasons[i] if batch_target_seasons and i < len(batch_target_seasons) else None
            keywords = (
                batch_expected_keywords[i] if batch_expected_keywords and i < len(batch_expected_keywords) else None
            )

            m = cls.evaluate_query(
                ret,
                rel,
                k=k,
                candidates=cands,
                target_entities=entities,
                target_season=season,
                expected_keywords=keywords,
            )
            sum_p += m.precision_at_k
            sum_r += m.recall_at_k
            sum_r1 += m.recall_at_1
            sum_r3 += m.recall_at_3
            sum_rr += m.mrr
            sum_ndcg += m.ndcg
            sum_ndcg10 += m.ndcg_at_10
            sum_hits += m.hit_rate
            sum_entity += m.entity_match_rate
            sum_temporal += m.temporal_fidelity_rate
            sum_containment += m.containment_rate

        return RagEvaluationMetrics(
            precision_at_k=sum_p / n,
            recall_at_k=sum_r / n,
            recall_at_1=sum_r1 / n,
            recall_at_3=sum_r3 / n,
            mrr=sum_rr / n,
            ndcg=sum_ndcg / n,
            ndcg_at_10=sum_ndcg10 / n,
            hit_rate=sum_hits / n,
            entity_match_rate=sum_entity / n,
            temporal_fidelity_rate=sum_temporal / n,
            containment_rate=sum_containment / n,
            sample_count=n,
        )
