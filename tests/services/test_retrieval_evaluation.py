"""Tests for offline retrieval evaluation metrics."""

from __future__ import annotations

from src.services.retrieval_evaluation import (
    GoldenQuery,
    evaluate_dataset,
    evaluate_routing_dataset,
    evaluate_variants,
    precision_at_k,
    recall_at_k,
    reciprocal_rank,
)
from src.services.retrieval_evaluation import _result_id


def test_metrics_calculate_expected_values() -> None:
    """Calculate Recall@K, Precision@K, and reciprocal rank."""
    retrieved = [{"chunk_id": "a"}, {"chunk_id": "b"}, {"chunk_id": "c"}]
    relevant = {"b", "c"}

    assert recall_at_k(retrieved, relevant, 2) == 0.5
    assert precision_at_k(retrieved, relevant, 2) == 0.5
    assert reciprocal_rank(retrieved, relevant) == 0.5


def test_evaluate_dataset_aggregates_queries() -> None:
    """Aggregate per-query metrics into a stable report."""
    queries = [
        GoldenQuery("first", ("a",)),
        GoldenQuery("second", ("z",)),
    ]

    def retrieve(query: GoldenQuery, _top_k: int) -> list[dict[str, str]]:
        """Return deterministic fixture results."""
        return [{"chunk_id": "a"}] if query.query == "first" else [{"chunk_id": "b"}]

    report = evaluate_dataset(queries, retrieve, top_k=1)
    assert report["query_count"] == 2
    assert report["recall_at_k"] == 0.5
    assert report["mrr"] == 0.5
    assert report["hit_rate"] == 0.5


def test_evaluate_variants_uses_the_same_golden_labels() -> None:
    """Compare named retrievers without changing the evaluation contract."""
    queries = [GoldenQuery("first", ("a",))]
    reports = evaluate_variants(
        queries,
        {
            "bm25": lambda _query, _top_k: [{"chunk_id": "a"}],
            "vector": lambda _query, _top_k: [{"chunk_id": "b"}],
        },
        top_k=1,
    )
    assert reports["bm25"]["mrr"] == 1.0
    assert reports["vector"]["mrr"] == 0.0


def test_evaluate_routing_dataset_tracks_false_positive_entities() -> None:
    """Measure routing accuracy and entity-negative false positives."""
    queries = [
        GoldenQuery(
            "규정",
            (),
            intent="RULE_QUERY",
            expected_route="DOCUMENT",
            expected_entities={"player_name": None},
        ),
    ]
    report = evaluate_routing_dataset(
        queries,
        lambda _query: {
            "intent": "RULE_QUERY",
            "route": "DOCUMENT",
            "entities": {"player_name": "규정"},
        },
    )
    assert report["intent_accuracy"] == 1.0
    assert report["route_accuracy"] == 1.0
    assert report["entity_false_positive_rate"] == 1.0


def test_result_id_prefers_cross_index_source_identity() -> None:
    """Use the same source key for vector results and golden annotations."""
    assert _result_id({"id": 99, "chunk_id": "game:g1"}) == "game:g1"
