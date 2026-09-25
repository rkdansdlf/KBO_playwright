"""Create a read-only RAG rebuild candidate plan without embedding or writes."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections import Counter
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.constants import RAG_EMBEDDING_DIMENSION
from src.services.rag_incremental_selection import (
    IncrementalDecisionOptions,
    RagCandidateDecision,
    decide_incremental_candidate,
    embedding_fingerprint,
)
from src.services.rag_index_identity import current_index_version

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from contextlib import AbstractContextManager

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_SAMPLE_LIMIT = 20
_UNKNOWN_VECTOR = "UNKNOWN"
_AVAILABLE_VECTOR = "AVAILABLE"


@dataclass(frozen=True, slots=True)
class PlannerSourceOptions:
    """Store per-source selector and sampling options."""

    require_vector: bool
    require_primary_embedding: bool
    sample_limit: int


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse read-only planner arguments."""
    from src.cli.rag.build_rag_index import rag_source_choices

    parser = argparse.ArgumentParser(description="Plan RAG rebuild candidates without writes")
    parser.add_argument("--source", choices=rag_source_choices(), default="all")
    parser.add_argument("--season", type=int)
    parser.add_argument("--limit", type=int, help="Limit generated source rows; disables complete-scope reporting")
    parser.add_argument(
        "--vector-state",
        choices=("unknown", "required"),
        default="unknown",
        help="unknown emits sparse-only evidence; required fails closed without a vector backend",
    )
    parser.add_argument("--sample", type=int, default=_SAMPLE_LIMIT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args(argv)


def _desired_fingerprint() -> str | None:
    """Return the configured embedding fingerprint without calling a provider."""
    model = os.getenv("EMBEDDING_MODEL")
    if not model:
        return None
    return embedding_fingerprint(model, RAG_EMBEDDING_DIMENSION, os.getenv("RAG_CHUNKING_VERSION", "rag-v1"))


def _vector_context(
    *,
    required: bool,
) -> tuple[AbstractContextManager[Session | None], str, bool, bool]:
    """Resolve vector session state for read-only comparison."""
    from src.db.vector_engine import get_vector_session, is_oracle_vector_backend, is_pgvector_available

    if not required:
        return nullcontext(None), _UNKNOWN_VECTOR, False, False
    if is_oracle_vector_backend():
        return nullcontext(None), _AVAILABLE_VECTOR, False, True
    if not is_pgvector_available():
        message = "vector state required but no vector backend is available"
        raise RuntimeError(message)
    return get_vector_session(), _AVAILABLE_VECTOR, True, False


def _summarize_decisions(
    decisions: Iterator[RagCandidateDecision],
    *,
    sample_limit: int,
) -> dict[str, object]:
    """Summarize decisions and retain a bounded candidate key sample."""
    reasons: Counter[str] = Counter()
    candidate_keys: list[str] = []
    generated_count = 0
    candidate_count = 0
    blocked = 0
    metadata_gap = 0
    for decision in decisions:
        generated_count += 1
        reasons[decision.reason] += 1
        if decision.blocked:
            blocked += 1
        if decision.metadata_repair and not decision.should_reembed:
            metadata_gap += 1
        if decision.should_reembed:
            candidate_count += 1
            if len(candidate_keys) < sample_limit:
                candidate_keys.append(decision.source_key)
    return {
        "generated_chunks": generated_count,
        "reason_counts": dict(sorted(reasons.items())),
        "candidate_count": candidate_count,
        "blocked_count": blocked,
        "metadata_gap_count": metadata_gap,
        "candidate_keys": candidate_keys,
    }


def _plan_source(
    source_name: str,
    chunk_iter: Iterator[Mapping[str, object]],
    index_session: Session,
    vector_session: Session | None,
    options: PlannerSourceOptions,
) -> dict[str, object]:
    """Plan candidates for one source without persisting anything."""
    from src.cli.rag.build_rag_index import (
        load_incremental_index_states,
        load_incremental_vector_states,
        source_table_for_source,
    )

    source_table = source_table_for_source(source_name)
    canonical_states = load_incremental_index_states(index_session, source_table)
    vector_states = load_incremental_vector_states(vector_session, source_table) if vector_session is not None else {}
    decision_options = IncrementalDecisionOptions(
        desired_index_version=current_index_version(),
        desired_embedding_fingerprint=_desired_fingerprint(),
        require_vector=options.require_vector,
        require_primary_embedding=options.require_primary_embedding,
    )

    def decisions() -> Iterator[RagCandidateDecision]:
        for chunk in chunk_iter:
            key = (str(chunk.get("source_table") or ""), str(chunk.get("source_row_id") or ""))
            yield decide_incremental_candidate(
                chunk,
                canonical_states.get(key),
                vector_states.get(key),
                decision_options,
            )

    summary = _summarize_decisions(decisions(), sample_limit=options.sample_limit)
    return {"source": source_name, **summary}


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only planner."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)
    if args.sample < 0:
        sys.stderr.write("plan_error: --sample must not be negative\n")
        return 2

    try:
        vector_context, vector_state, require_vector, require_primary_embedding = _vector_context(
            required=args.vector_state == "required"
        )
    except (RuntimeError, SQLAlchemyError, OSError) as exc:
        sys.stderr.write(f"plan_error: {exc}\n")
        return 2

    from src.cli.rag.build_rag_index import rag_source_map
    from src.db.engine import get_rag_index_session, get_rag_source_session

    source_map = rag_source_map()
    sources = list(source_map) if args.source == "all" else [args.source]
    source_results: list[dict[str, object]] = []
    has_errors = False
    source_options = PlannerSourceOptions(
        require_vector=require_vector,
        require_primary_embedding=require_primary_embedding,
        sample_limit=args.sample,
    )
    with (
        get_rag_source_session() as source_session,
        get_rag_index_session() as index_session,
        vector_context as vector_session,
    ):
        for source_name in sources:
            try:
                chunk_iter = source_map[source_name](source_session, args.season, args.limit)
                result = _plan_source(
                    source_name,
                    chunk_iter,
                    index_session,
                    vector_session,
                    source_options,
                )
                source_results.append(result)
            except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError) as exc:
                has_errors = True
                source_results.append({"source": source_name, "error": type(exc).__name__})
                logger.warning("source planning failed: %s (%s)", source_name, type(exc).__name__)

    totals = {
        "source_count": len(source_results),
        "candidate_count": sum(int(row.get("candidate_count", 0) or 0) for row in source_results),
        "blocked_count": sum(int(row.get("blocked_count", 0) or 0) for row in source_results),
        "metadata_gap_count": sum(int(row.get("metadata_gap_count", 0) or 0) for row in source_results),
        "error_count": sum(1 for row in source_results if row.get("error")),
    }
    payload = {
        "read_only": True,
        "vector_state": vector_state,
        "index_version": current_index_version(),
        "complete_scope": args.limit is None,
        "totals": totals,
        "sources": source_results,
        "write_operations": [],
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            f"read_only=true vector_state={vector_state} sources={totals['source_count']} "
            f"candidates={totals['candidate_count']} errors={totals['error_count']}\n"
        )
        for row in source_results:
            if row.get("error"):
                sys.stdout.write(f"{row['source']}: ERROR {row['error']}\n")
            else:
                sys.stdout.write(
                    f"{row['source']}: generated={row.get('generated_chunks', 0)} "
                    f"candidates={row.get('candidate_count', 0)} reasons={row.get('reason_counts', {})}\n"
                )
    return 1 if has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
