"""Offline metrics for evaluating KBO retrieval changes on a golden set."""

from __future__ import annotations

import json
import math
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GoldenQuery:
    """Represent one labeled retrieval query."""

    query: str
    relevant_chunk_ids: tuple[str, ...]
    intent: str | None = None
    filters: dict[str, Any] | None = None
    case_id: str | None = None
    expected_route: str | None = None
    expected_entities: dict[str, Any] | None = None
    tags: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> GoldenQuery:
        """Build a golden query from a JSON-compatible mapping."""
        tags = tuple(str(tag) for tag in payload.get("tags", []))
        filters = dict(payload.get("filters") or {})
        tag_categories = {
            "rules": "rulebook",
            "player": "player_profile",
            "batting": "season_batting",
            "pitching": "season_pitching",
            "game": "game_result",
            "standings": "standings",
            "stadium": "stadium_facility",
            "award": "award",
            "history": "history",
            "team": "team_profile",
        }
        for tag in tags:
            if tag in tag_categories:
                filters.setdefault("document_type", tag_categories[tag])
                break
        return cls(
            query=str(payload["query"]),
            relevant_chunk_ids=tuple(str(value) for value in payload.get("relevantChunkIds", [])),
            intent=payload.get("expectedIntent", payload.get("expected_intent", payload.get("intent"))),
            filters=filters or None,
            case_id=payload.get("id"),
            expected_route=payload.get("expectedRoute", payload.get("expected_route")),
            expected_entities=payload.get("expectedEntities", payload.get("expected_entities")),
            tags=tags,
        )


def load_golden_queries(path: str | Path) -> list[GoldenQuery]:
    """Load golden retrieval queries from a JSON array."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        message = "Golden query dataset must be a JSON array"
        raise TypeError(message)
    return [GoldenQuery.from_mapping(item) for item in payload]


def _result_id(result: object) -> str:
    """Extract a stable chunk ID from a result object or mapping."""
    if isinstance(result, Mapping):
        return str(result.get("chunk_id") or result.get("chunkId") or result.get("id"))
    return str(getattr(result, "chunk_id", getattr(result, "id", result)))


def _enum_value(value: object) -> object:
    """Return an enum value while accepting plain strings in fixture plans."""
    return getattr(value, "value", value)


def recall_at_k(retrieved: Sequence[object], relevant: set[str], k: int) -> float:
    """Calculate recall at K for one query."""
    if not relevant:
        return 0.0
    found = {_result_id(result) for result in retrieved[:k]}
    return len(found & relevant) / len(relevant)


def precision_at_k(retrieved: Sequence[object], relevant: set[str], k: int) -> float:
    """Calculate precision at K for one query."""
    if k <= 0:
        return 0.0
    found = {_result_id(result) for result in retrieved[:k]}
    return len(found & relevant) / k


def reciprocal_rank(retrieved: Sequence[object], relevant: set[str]) -> float:
    """Calculate reciprocal rank for the first relevant result."""
    for rank, result in enumerate(retrieved, start=1):
        if _result_id(result) in relevant:
            return 1.0 / rank
    return 0.0


def evaluate_dataset(
    queries: Sequence[GoldenQuery],
    retrieve: Callable[[GoldenQuery, int], Sequence[object]],
    *,
    top_k: int = 5,
) -> dict[str, Any]:
    """Evaluate a retriever and return aggregate Recall, Precision, and MRR."""
    if not queries:
        return {
            "query_count": 0,
            "top_k": top_k,
            "recall_at_k": 0.0,
            "precision_at_k": 0.0,
            "mrr": 0.0,
            "hit_rate": 0.0,
        }

    recalls: list[float] = []
    precisions: list[float] = []
    ranks: list[float] = []
    latencies_ms: list[float] = []
    hits = 0
    for golden in queries:
        started = time.perf_counter()
        retrieved = list(retrieve(golden, top_k))
        latencies_ms.append((time.perf_counter() - started) * 1000)
        relevant = set(golden.relevant_chunk_ids)
        recalls.append(recall_at_k(retrieved, relevant, top_k))
        precisions.append(precision_at_k(retrieved, relevant, top_k))
        rank = reciprocal_rank(retrieved, relevant)
        ranks.append(rank)
        hits += rank > 0

    count = len(queries)
    return {
        "query_count": count,
        "top_k": top_k,
        "recall_at_k": sum(recalls) / count,
        "precision_at_k": sum(precisions) / count,
        "mrr": sum(ranks) / count,
        "hit_rate": hits / count,
        "latency_ms": {
            "p50": round(_percentile(latencies_ms, 0.50), 3),
            "p95": round(_percentile(latencies_ms, 0.95), 3),
            "max": round(max(latencies_ms), 3),
        },
    }


def evaluate_variants(
    queries: Sequence[GoldenQuery],
    retrievers: Mapping[str, Callable[[GoldenQuery, int], Sequence[object]]],
    *,
    top_k: int = 5,
) -> dict[str, dict[str, Any]]:
    """Evaluate multiple retrieval implementations against the same labels."""
    return {name: evaluate_dataset(queries, retrieve, top_k=top_k) for name, retrieve in retrievers.items()}


def _percentile(values: Sequence[float], fraction: float) -> float:
    """Return a nearest-rank percentile for a non-empty latency sample."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def _plan_value(plan: object, key: str) -> object:
    """Read a routing field from a QueryPlan or serialized mapping."""
    if isinstance(plan, Mapping):
        analysis = plan.get("analysis")
        analysis = analysis if isinstance(analysis, Mapping) else {}
        if key in {"intent", "route"}:
            return plan.get(key) or analysis.get(key)
        entities = plan.get("entities") or analysis.get("entities", {})
        return entities.get(key) if isinstance(entities, Mapping) else None
    entities = getattr(plan, "entities", None)
    extracted = getattr(entities, "extracted", None)
    values = {
        "intent": _enum_value(getattr(plan, "intent", None)),
        "route": _enum_value(getattr(plan, "route", None)),
        "player_id": getattr(entities, "player_id", None),
        "ambiguous_player": getattr(entities, "ambiguous_player", False),
        "player_name": getattr(extracted, "player_name", None),
        "team_id": getattr(extracted, "team_id", None),
        "season_year": getattr(extracted, "season_year", None),
    }
    return values.get(key)


def evaluate_routing_dataset(
    queries: Sequence[GoldenQuery],
    plan_query: Callable[[GoldenQuery], object],
) -> dict[str, Any]:
    """Measure intent, route, and entity resolution accuracy."""
    if not queries:
        return {
            "query_count": 0,
            "intent_accuracy": 0.0,
            "route_accuracy": 0.0,
            "entity_accuracy": 0.0,
            "entity_false_positive_rate": 0.0,
            "entity_false_positive_count": 0,
        }

    intent_hits = 0
    route_hits = 0
    entity_total = 0
    entity_hits = 0
    false_positive = 0
    negative_entity_labels = 0
    intent_counts: Counter[str] = Counter()
    tag_counts: Counter[str] = Counter()
    for golden in queries:
        plan = plan_query(golden)
        if golden.intent:
            intent_counts[golden.intent] += 1
        tag_counts.update(golden.tags)
        intent_hits += int(bool(golden.intent) and _plan_value(plan, "intent") == golden.intent)
        route_hits += int(bool(golden.expected_route) and _plan_value(plan, "route") == golden.expected_route)
        for field, expected in (golden.expected_entities or {}).items():
            actual = _plan_value(plan, field)
            entity_total += 1
            entity_hits += int(actual == expected)
            if expected is None:
                negative_entity_labels += 1
                false_positive += int(actual is not None)

    count = len(queries)
    return {
        "query_count": count,
        "intent_accuracy": intent_hits / count,
        "route_accuracy": route_hits / count,
        "entity_accuracy": entity_hits / entity_total if entity_total else 0.0,
        "entity_false_positive_rate": false_positive / negative_entity_labels if negative_entity_labels else 0.0,
        "entity_false_positive_count": false_positive,
        "expected_intent_counts": dict(intent_counts),
        "tag_counts": dict(tag_counts),
    }
