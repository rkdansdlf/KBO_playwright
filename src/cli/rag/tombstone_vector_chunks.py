"""Tombstone chunks in the vector store, alongside ``tombstone_rag_chunks``.

The primary-side tool retires rows in the sparse store only. Since the vectors
moved to pgvector a chunk lives in two places under one identity, so this covers
the other half: the dense leg keeps serving deleted content otherwise, and the
reconciliation gate reports an index_status mismatch for every retired key.

Reads take ``--dry-run`` (the default, which inspects without writing). A write
needs ``--apply`` and, in production, ``RAG_INDEX_ALLOW_PRODUCTION_WRITE=1`` --
the same switch ``tombstone_rag_chunks`` and the rekey tools already require, so
an operator who has opened that door has not opened it here by accident.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.services.rag_vector_tombstone import inspect_vector_tombstones, tombstone_vector_rows

if TYPE_CHECKING:
    from collections.abc import Sequence

GUARD_ENV = "RAG_INDEX_ALLOW_PRODUCTION_WRITE"


def _load_source_keys(keys: Sequence[str], manifest_path: Path | None) -> tuple[str, ...]:
    """Read keys from the command line and any manifest."""
    result = list(keys)
    if manifest_path is not None:
        payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        for source in payload.get("sources", []):
            result.extend(source.get("deleted_identities", []))
    seen: set[str] = set()
    deduped = []
    for item in result:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return tuple(deduped)


def _guard_error(*, apply: bool, vector_url: str) -> str | None:
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
    parser.add_argument("--keys", nargs="*", default=[], help="source_table:source_row_id pairs")
    parser.add_argument("--manifest", type=Path, default=None, help="Plan with sources[].deleted_identities")
    parser.add_argument("--apply", action="store_true", help="Persist the tombstones")
    parser.add_argument("--dry-run", action="store_true", help="Inspect without writing (default)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Inspect or apply vector-store tombstones."""
    args = build_parser().parse_args(argv)
    keys = _load_source_keys(args.keys, args.manifest)

    vector_url = os.getenv("PGVECTOR_URL", "")
    if not vector_url:
        sys.stderr.write("PGVECTOR_URL must be set\n")
        return 1
    refusal = _guard_error(apply=args.apply, vector_url=vector_url)
    if refusal is not None:
        sys.stderr.write(f"refusing write: {refusal}\n")
        return 3

    from src.db.vector_engine import get_vector_session

    with get_vector_session() as session:
        if args.apply and not args.dry_run:
            results = tombstone_vector_rows(session, keys)
        else:
            results = inspect_vector_tombstones(session, keys)

    tombstoned = sum(1 for r in results if r.final_status == "DELETED")
    already = sum(1 for r in results if r.found and r.previous_status == "DELETED")
    missing = sum(1 for r in results if not r.found)
    for res in results:
        sys.stdout.write(f"{res.to_dict()}\n")
    sys.stdout.write(f"tombstoned={tombstoned} already_deleted={already} missing={missing}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
