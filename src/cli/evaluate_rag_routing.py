"""Evaluate QueryRouter intent, route, and entity-resolution accuracy."""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.db.engine import get_db_session
from src.services.query_router import QueryRouter
from src.services.retrieval_evaluation import GoldenQuery, evaluate_routing_dataset, load_golden_queries

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run a labeled routing evaluation without changing indexed data."""
    parser = argparse.ArgumentParser(description="Evaluate KBO QueryRouter and entity resolution")
    parser.add_argument("--dataset", required=True, help="Path to a routing golden query JSON array")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    args = parser.parse_args(argv)
    queries = load_golden_queries(args.dataset)
    if not queries:
        return _error("routing dataset is empty")
    if any(not query.intent or not query.expected_route for query in queries):
        return _error("every routing case requires expectedIntent and expectedRoute")

    with get_db_session() as session:
        router = QueryRouter(session)

        def plan_query(golden: GoldenQuery) -> object:
            """Build a production routing plan for one labeled query."""
            return router.plan(golden.query, filters=golden.filters)

        report = evaluate_routing_dataset(queries, plan_query)

    if args.as_json:
        sys.stdout.write(json.dumps(report, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            "queries={query_count} intent={intent_accuracy:.4f} route={route_accuracy:.4f} "
            "entity={entity_accuracy:.4f} entity_fp={entity_false_positive_rate:.4f}\n".format(**report)
        )
    return 0


def _error(message: str) -> int:
    """Render a validation error and return the data-contract failure code."""
    sys.stderr.write(f"evaluation_error: {message}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
