"""Apply a rekey manifest to the vector store, alongside ``apply_rag_rekey``.

Reads the same census manifest and performs the same two mutations on the
pgvector store: ``SAFE_REKEY`` moves ``source_row_id`` to the natural key, and
``TARGET_EXISTS_SAME_CONTENT`` tombstones the redundant legacy row. Everything
else is skipped, exactly as the sparse side skips it.

Dry-run is the default. ``--apply`` against a production target requires
``RAG_INDEX_ALLOW_PRODUCTION_WRITE=1``, the same switch ``apply_rag_rekey``
requires, so an operator who has opened the door for one has not silently
opened it for the other.

Run this and ``apply_rag_rekey`` for the same manifest. Applying one alone
leaves the two stores holding different keys for the same chunk.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.config.env_loader import load_project_env
from src.services.rag_vector_rekey import apply_entries, load_entries, summarize_manifest

if TYPE_CHECKING:
    from collections.abc import Sequence

GUARD_ENV = "RAG_INDEX_ALLOW_PRODUCTION_WRITE"


def _write(payload: dict[str, object]) -> None:
    """Print one JSON line to stdout."""
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _error(message: str) -> None:
    """Print one error line to stderr."""
    sys.stderr.write(message + "\n")


def guard_error(*, apply: bool, vector_url: str) -> str | None:
    """Return why this run may not write, or ``None`` when it may."""
    if not apply:
        return None
    if "kbo_dev.db" in vector_url:
        return "refusing to write the local development database"
    if os.getenv("RAG_TARGET_ENV", "local").strip().lower() == "production" and os.getenv(GUARD_ENV) != "1":
        return f"production --apply requires {GUARD_ENV}=1"
    return None


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True, help="Census manifest JSON to apply.")
    parser.add_argument("--apply", action="store_true", help="Persist the mutations.")
    parser.add_argument("--json", action="store_true", help="Render the report as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the vector-side rekey preview or the guarded apply."""
    load_project_env()
    args = build_parser().parse_args(argv)
    entries = load_entries(args.manifest)
    counts = summarize_manifest(entries)
    _write({"event": "plan", "entries": len(entries), "by_disposition": counts, "apply": bool(args.apply)})

    vector_url = os.getenv("PGVECTOR_URL", "")
    if not vector_url:
        _error("PGVECTOR_URL must be set")
        return 1
    refusal = guard_error(apply=args.apply, vector_url=vector_url)
    if refusal is not None:
        _error(f"refusing write: {refusal}")
        return 3

    from src.db.vector_engine import get_vector_session
    from src.scheduler import locks as scheduler_locks

    lock_acquired = False
    try:
        if args.apply:
            # Mirrors apply_rag_rekey: an apply is a maintenance mutation, and the
            # two tools are meant to run in one window rather than interleaved
            # with the scheduled RAG jobs.
            lock_acquired = scheduler_locks.MAINTENANCE_LOCK.acquire(blocking=True, timeout=300)
            if not lock_acquired:
                _error("could not acquire maintenance lock")
                return 1
        with get_vector_session() as session:
            report = apply_entries(session, entries, dry_run=not args.apply)
    finally:
        if lock_acquired:
            scheduler_locks.MAINTENANCE_LOCK.release()
    _write(
        {
            "event": "applied" if args.apply else "preview",
            "rekeyed": report.rekeyed,
            "tombstoned": report.tombstoned,
            "planned_rekeyed": report.planned_rekeyed,
            "planned_tombstoned": report.planned_tombstoned,
            "already_applied": report.already_applied,
            "skipped_unsupported": report.skipped_unsupported,
            "missing": report.missing,
            "conflicted": report.conflicted,
            "failed": report.failed,
            "summary": report.summary,
        }
    )
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
