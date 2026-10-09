"""Embed the identities the Oracle copy could not supply, into the vector store.

Reads the sparse store (``DATABASE_URL``), finds the ``ACTIVE`` identities the
pgvector store does not hold, embeds their ``content`` with the configured
provider, and upserts them.

Preview is the default: without ``--apply`` **and**
``KBO_ALLOW_RAG_VECTOR_MIGRATION=1`` nothing is written and the command exits 3,
sharing the guard the copy uses so there is one variable to remember for
anything that writes vectors.

``--use-embedding-cache`` is opt-in. The cache lives in the operational
database, and a one-off gap backfill should not quietly add rows there; 127
chunks are cheap enough to embed again if this is ever re-run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import TYPE_CHECKING

from src.db.engine import create_engine_for_url
from src.services.rag_vector_backfill import (
    DEFAULT_BATCH_SIZE,
    backfill_gap,
    iter_gap_chunks,
    load_target_keys,
)
from src.services.rag_vector_migration import verify_target

if TYPE_CHECKING:
    from src.services.rag_vector_backfill import BackfillReport

GUARD_ENV = "KBO_ALLOW_RAG_VECTOR_MIGRATION"
EXIT_GUARD_DENIED = 3
EXIT_GAP_TOO_LARGE = 2
DEFAULT_MAX_GAP = 5000


def _write(payload: dict[str, object]) -> None:
    """Print one JSON line to stdout."""
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _error(message: str) -> None:
    """Print one error line to stderr."""
    sys.stderr.write(message + "\n")


def gap_guard_message(gap_rows: int, max_gap: int) -> str | None:
    """Return why this gap is too large to embed, or ``None`` when it is fine.

    The gap is only correct once the copy has finished. Run against a store that
    is still filling, the missing set is every row the copy has not reached yet
    -- which looks like a legitimate backfill and would spend real money on rows
    that are about to arrive by themselves.
    """
    if gap_rows <= max_gap:
        return None
    return (
        f"gap of {gap_rows} chunks exceeds --max-gap={max_gap}; "
        "a copy still in progress looks exactly like this, so embedding now would pay for rows "
        "the copy is about to deliver. Re-check, or pass --allow-large-gap if the gap is real."
    )


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Chunks per embed batch.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after embedding this many chunks.")
    parser.add_argument("--max-gap", type=int, default=DEFAULT_MAX_GAP, help="Refuse a gap larger than this.")
    parser.add_argument("--allow-large-gap", action="store_true", help="Embed a gap larger than --max-gap.")
    parser.add_argument("--use-embedding-cache", action="store_true", help="Also write embeddings to the cache table.")
    parser.add_argument("--apply", action="store_true", help="Write the backfilled vectors.")
    return parser


def _count_gap(source_engine: object, target_engine: object) -> int:
    """Count the identities the vector store is missing."""
    target_keys = load_target_keys(target_engine)
    return sum(1 for _ in iter_gap_chunks(source_engine, target_keys))


def _progress(report: BackfillReport) -> None:
    """Print each batch's progress as one JSON line."""
    _write({"event": "progress", "summary": report.summary})


def main(argv: list[str] | None = None) -> int:
    """Run the gap backfill preview or the guarded backfill."""
    args = build_parser().parse_args(argv)
    source_url = os.getenv("DATABASE_URL")
    target_url = os.getenv("PGVECTOR_URL")
    if not source_url or not target_url:
        _error("DATABASE_URL and PGVECTOR_URL must both be set")
        return 1

    source_engine = create_engine_for_url(source_url)
    target_engine = create_engine_for_url(target_url)

    preview = {"event": "apply" if args.apply else "preview", "gap_rows": _count_gap(source_engine, target_engine)}
    _write(preview)
    refusal = gap_guard_message(preview["gap_rows"], args.max_gap)  # type: ignore[arg-type]
    if refusal is not None and not args.allow_large_gap:
        _error(f"refusing write: {refusal}")
        return EXIT_GAP_TOO_LARGE
    if not args.apply:
        _error(f"refusing write: pass --apply and set {GUARD_ENV}=1")
        return EXIT_GUARD_DENIED
    if os.getenv(GUARD_ENV) != "1":
        _error(f"refusing write: --apply requires {GUARD_ENV}=1")
        return EXIT_GUARD_DENIED

    from src.services.embedding_service import EmbeddingService

    report = backfill_gap(
        source_engine,
        target_engine,
        EmbeddingService(cache_enabled=args.use_embedding_cache),
        batch_size=args.batch_size,
        limit=args.limit,
        on_progress=_progress,
    )
    verification = verify_target(target_engine)
    _write(
        {
            "event": "done",
            "source_rows": report.source_rows,
            "gap_rows": report.gap_rows,
            "embedded": report.embedded,
            "copied": report.copied,
            "failed": report.failed,
            "batches": report.batches,
            "failure_samples": report.failure_samples[:5],
            "verify": {
                "rows": verification.rows,
                "with_embedding": verification.with_embedding,
                "wrong_dimension": verification.wrong_dimension,
                "zero_vectors": verification.zero_vectors,
                "is_clean": verification.is_clean,
            },
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
