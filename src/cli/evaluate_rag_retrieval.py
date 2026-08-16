"""Evaluate hybrid RAG retrieval against a labeled golden query dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import get_db_session
from src.db.vector_engine import get_vector_session, is_pgvector_available
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_vector import RagChunkVector
from src.repositories.vector_search_repository import VectorSearchRepository
from src.services.embedding_service import EmbeddingService
from src.services.hybrid_retriever import HybridRetriever
from src.services.rag_eval_corpus import DeterministicEmbeddingService
from src.services.rag_index_identity import current_index_version
from src.services.rag_search_engine import RagSearchEngine
from src.services.retrieval_evaluation import GoldenQuery, evaluate_variants, load_golden_queries

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run an offline retrieval evaluation without modifying indexed data."""
    args = _parse_args(argv)
    try:
        queries = load_golden_queries(args.dataset)
        error = _validate_request(queries, args)
    except (OSError, TypeError, ValueError, SQLAlchemyError, RuntimeError) as exc:
        sys.stderr.write(f"evaluation_error: {_error_message(exc)}\n")
        return 2
    if error:
        sys.stderr.write(f"evaluation_error: {error}\n")
        return 2

    try:
        reports = _evaluate_queries(queries, args)
    except (OSError, TypeError, ValueError, SQLAlchemyError, RuntimeError) as exc:
        sys.stderr.write(f"evaluation_error: {_error_message(exc)}\n")
        return 2
    report = next(iter(reports.values())) if not args.all_variants else {"variants": reports}
    report.update(
        {
            "dataset": str(args.dataset),
            "embedding_mode": args.embedding_mode,
            "top_k": args.top_k,
            "metadata": {
                "dataset_sha256": _file_sha256(args.dataset),
                "corpus_sha256": _file_sha256(args.corpus) if args.corpus else None,
                "git_commit": os.getenv("GITHUB_SHA") or os.getenv("GIT_COMMIT") or "unknown",
                "index_version": current_index_version(),
                "embedding_model": _embedding_model_name(args.embedding_mode),
            },
        }
    )
    _render_report(report, args)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 1 if _below_threshold(report, args) else 0


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse retrieval evaluation options."""
    parser = argparse.ArgumentParser(description="Evaluate KBO RAG retrieval")
    parser.add_argument("--dataset", required=True, help="Path to a golden query JSON array")
    parser.add_argument("--corpus", type=Path, default=None, help="Optional source corpus manifest to fingerprint")
    parser.add_argument("--top-k", type=int, default=5, help="Evaluation cutoff")
    parser.add_argument(
        "--variant",
        choices=("bm25", "vector", "hybrid", "resolver_hybrid"),
        default="resolver_hybrid",
        help="Retrieval implementation to evaluate",
    )
    parser.add_argument("--all-variants", action="store_true", help="Evaluate all comparable implementations")
    parser.add_argument(
        "--require-corpus",
        action="store_true",
        help="Fail with exit 2 when golden IDs are absent from the selected index",
    )
    parser.add_argument(
        "--embedding-mode",
        choices=("configured", "deterministic"),
        default="configured",
        help="Embedding provider; deterministic is reproducible for the fixture corpus",
    )
    parser.add_argument("--min-recall", type=float, default=None, help="Fail with exit 1 below this Recall@K")
    parser.add_argument("--min-mrr", type=float, default=None, help="Fail with exit 1 below this MRR")
    parser.add_argument("--output", type=Path, default=None, help="Write the JSON evaluation artifact to this path")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    return parser.parse_args(argv)


def _validate_request(queries: list[GoldenQuery], args: argparse.Namespace) -> str | None:
    """Validate dataset, vector dependency, and optional indexed-corpus requirements."""
    if not queries:
        return "retrieval dataset is empty"
    if any(not query.relevant_chunk_ids for query in queries):
        return "every retrieval case requires relevantChunkIds"
    if args.all_variants and not is_pgvector_available():
        return "--all-variants requires pgvector"
    if args.variant == "vector" and not is_pgvector_available():
        return "vector variant requires pgvector"
    if args.require_corpus:
        missing = _missing_corpus_ids(queries, require_vector=args.all_variants or args.variant == "vector")
        if missing:
            return f"golden IDs missing from indexed corpus: {', '.join(missing)}"
    return None


def _render_report(report: dict[str, object], args: argparse.Namespace) -> None:
    """Render a retrieval report in JSON or concise text form."""
    if args.as_json:
        sys.stdout.write(json.dumps(report, ensure_ascii=False) + "\n")
    elif args.all_variants:
        for name, variant_report in report["variants"].items():
            sys.stdout.write(
                "{name}: recall={recall_at_k:.4f} precision={precision_at_k:.4f} mrr={mrr:.4f}\n".format(
                    name=name, **variant_report
                )
            )
    else:
        sys.stdout.write(
            "queries={query_count} k={top_k} recall={recall_at_k:.4f} precision={precision_at_k:.4f} "
            "mrr={mrr:.4f}\n".format(**report)
        )


def _evaluate_queries(queries: list[GoldenQuery], args: argparse.Namespace) -> dict[str, dict[str, object]]:
    """Evaluate the selected variants against one database session."""
    with get_db_session() as session:
        variants = _build_variants(
            session,
            include_vector=args.all_variants or args.variant == "vector",
            embedding_mode=args.embedding_mode,
        )
        selected = variants if args.all_variants else {args.variant: variants[args.variant]}
        return evaluate_variants(queries, selected, top_k=args.top_k)


def _build_variants(
    session: object,
    *,
    include_vector: bool,
    embedding_mode: str = "configured",
) -> dict[str, Callable[[GoldenQuery, int], Sequence[object]]]:
    """Build named lexical, dense, and hybrid retrieval callables."""
    bm25 = RagSearchEngine(session)
    vector_repo = VectorSearchRepository() if include_vector else None
    embedder = _build_embedding_service(embedding_mode) if vector_repo is not None else None
    resolver_hybrid = HybridRetriever(session, resolve_entities=True, embedding_service=embedder)
    hybrid = HybridRetriever(session, resolve_entities=False, embedding_service=embedder)

    def bm25_retrieve(golden: GoldenQuery, top_k: int) -> Sequence[object]:
        """Retrieve one query with lexical search only."""
        return bm25.search(golden.query, top_k=top_k, filters=golden.filters)

    def vector_retrieve(golden: GoldenQuery, top_k: int) -> Sequence[object]:
        """Retrieve one query with dense vector search only."""
        if vector_repo is None or embedder is None:
            return []
        filters = golden.filters or {}
        return vector_repo.search_by_cosine(
            query_vector=embedder.get_embedding(golden.query),
            top_k=top_k,
            team_id=filters.get("team_id"),
            season_year=filters.get("season_year"),
            source_table=filters.get("source_table"),
            league_type_code=filters.get("league_type_code"),
            document_type=filters.get("document_type"),
            game_date=filters.get("game_date"),
            player_id=filters.get("player_id"),
        )

    def hybrid_retrieve(golden: GoldenQuery, top_k: int) -> Sequence[object]:
        """Retrieve one query with the legacy hybrid path."""
        return hybrid.retrieve(query=golden.query, top_k=top_k, filters=golden.filters)

    def resolver_hybrid_retrieve(golden: GoldenQuery, top_k: int) -> Sequence[object]:
        """Retrieve one query with resolver-aware hybrid search."""
        return resolver_hybrid.retrieve(query=golden.query, top_k=top_k, filters=golden.filters)

    return {
        "bm25": bm25_retrieve,
        "vector": vector_retrieve,
        "hybrid": hybrid_retrieve,
        "resolver_hybrid": resolver_hybrid_retrieve,
    }


def _build_embedding_service(mode: str) -> DeterministicEmbeddingService | EmbeddingService:
    """Build the configured or offline deterministic embedding provider."""
    if mode == "deterministic":
        return DeterministicEmbeddingService()
    return EmbeddingService()


def _embedding_model_name(mode: str) -> str:
    """Return the stable embedding label stored in evaluation artifacts."""
    if mode == "deterministic":
        return DeterministicEmbeddingService.model_name
    return os.getenv("EMBEDDING_MODEL", "configured-provider")


def _file_sha256(path: str | Path) -> str:
    """Hash a golden dataset for reproducible result comparison."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _error_message(error: Exception) -> str:
    """Render database failures without dumping the generated SQL statement."""
    if isinstance(error, SQLAlchemyError):
        original = getattr(error, "orig", None)
        if original is not None:
            return f"{type(original).__name__}: {original}"
    return str(error)


def _missing_corpus_ids(queries: list[GoldenQuery], *, require_vector: bool) -> list[str]:
    """Return annotated IDs absent from the selected sparse/vector stores."""
    with get_db_session() as primary_session:
        primary_ids = {
            f"{row.source_table}:{row.source_row_id}"
            for row in primary_session.execute(select(RagChunk)).scalars().all()
        }
    available_ids = primary_ids
    if require_vector:
        with get_vector_session() as vector_session:
            available_ids = {
                f"{row.source_table}:{row.source_row_id}"
                for row in vector_session.execute(select(RagChunkVector)).scalars().all()
            }
    missing = {chunk_id for query in queries for chunk_id in query.relevant_chunk_ids if chunk_id not in available_ids}
    return sorted(missing)


def _below_threshold(report: dict[str, object], args: argparse.Namespace) -> bool:
    """Return whether a requested evaluation threshold failed."""
    if args.min_recall is None and args.min_mrr is None:
        return False
    reports = report.get("variants")
    candidates = reports.values() if isinstance(reports, dict) else (report,)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if args.min_recall is not None and float(candidate.get("recall_at_k", 0.0)) < args.min_recall:
            return True
        if args.min_mrr is not None and float(candidate.get("mrr", 0.0)) < args.min_mrr:
            return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
