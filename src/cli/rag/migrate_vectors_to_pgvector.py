"""Copy Oracle RAG dense vectors into the configured pgvector store.

Reads ``OCI_DB_URL`` (``KBO_APP.RAG_CHUNKS``) and upserts into ``PGVECTOR_URL``.
The preview is the default: without ``--apply`` **and**
``KBO_ALLOW_RAG_VECTOR_MIGRATION=1`` the command reports what it would copy and
exits without writing, matching the other data-reliability mutations here.

The copy is resumable -- ``--after-id`` is the last committed Oracle id, and the
upsert keys on ``(source_table, source_row_id)``, so re-running is safe.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import TYPE_CHECKING

from sqlalchemy import text

from src.db.engine import create_engine_for_url
from src.services.rag_vector_migration import (
    DEFAULT_BATCH_SIZE,
    migrate_vectors,
    verify_target,
)

if TYPE_CHECKING:
    from src.services.rag_vector_migration import MigrationReport, TargetVerification

GUARD_ENV = "KBO_ALLOW_RAG_VECTOR_MIGRATION"
EXIT_GUARD_DENIED = 3


def _write(payload: dict[str, object]) -> None:
    """Print one JSON line to stdout."""
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _error(message: str) -> None:
    """Print one error line to stderr."""
    sys.stderr.write(message + "\n")


def _count_pending(source: object, after_id: int) -> int:
    """Count source rows that still carry a vector."""
    return source.execute(
        text("SELECT COUNT(*) FROM KBO_APP.rag_chunks WHERE id > :after_id AND embedding_vector IS NOT NULL"),
        {"after_id": after_id},
    ).scalar_one()


def _count_target(target: object) -> int:
    """Count rows the destination store holds today."""
    return target.execute(text("SELECT COUNT(*) FROM rag_chunks")).scalar_one()


def _render_verification(verification: TargetVerification) -> dict[str, object]:
    """Render the destination check as a JSON-ready mapping."""
    return {
        "rows": verification.rows,
        "with_embedding": verification.with_embedding,
        "wrong_dimension": verification.wrong_dimension,
        "zero_vectors": verification.zero_vectors,
        "is_clean": verification.is_clean,
    }


def _progress(report: MigrationReport) -> None:
    """Print each batch's progress as one JSON line."""
    _write({"event": "progress", "summary": report.summary})


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tns-admin", default=os.getenv("TNS_ADMIN"), help="Oracle wallet/TNS directory.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows per upsert batch.")
    parser.add_argument("--after-id", type=int, default=0, help="Resume after this Oracle id.")
    parser.add_argument("--limit", type=int, default=None, help="Stop after this many source rows.")
    parser.add_argument("--verify-only", action="store_true", help="Report the destination state and exit.")
    parser.add_argument("--apply", action="store_true", help="Write to the pgvector store.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the migration preview or the guarded copy."""
    args = build_parser().parse_args(argv)
    source_url = os.getenv("OCI_DB_URL")
    target_url = os.getenv("PGVECTOR_URL")
    if not source_url or not target_url:
        _error("OCI_DB_URL and PGVECTOR_URL must both be set")
        return 1

    source_engine = create_engine_for_url(source_url, tns_admin=args.tns_admin)
    target_engine = create_engine_for_url(target_url)

    if args.verify_only:
        _write({"event": "verify", **_render_verification(verify_target(target_engine))})
        return 0

    with source_engine.connect() as source, target_engine.connect() as target:
        pending = _count_pending(source, args.after_id)
        existing = _count_target(target)
    preview = {
        "event": "preview" if not args.apply else "apply",
        "pending_source_rows": pending,
        "destination_rows": existing,
        "after_id": args.after_id,
        "batch_size": args.batch_size,
        "guard_env": GUARD_ENV,
    }
    if not args.apply:
        _write(preview)
        _error(f"refusing write: pass --apply and set {GUARD_ENV}=1")
        return EXIT_GUARD_DENIED
    if os.getenv(GUARD_ENV) != "1":
        _write(preview)
        _error(f"refusing write: --apply requires {GUARD_ENV}=1")
        return EXIT_GUARD_DENIED

    _write(preview)
    report = migrate_vectors(
        source_engine,
        target_engine,
        batch_size=args.batch_size,
        after_id=args.after_id,
        limit=args.limit,
        on_progress=_progress,
    )
    _write(
        {
            "event": "done",
            "scanned": report.scanned,
            "copied": report.copied,
            "failed": report.failed,
            "batches": report.batches,
            "last_source_id": report.last_source_id,
            "verify": _render_verification(verify_target(target_engine)),
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
