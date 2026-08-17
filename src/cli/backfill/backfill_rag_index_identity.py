"""Backfill shared RAG index identity metadata in dry-run mode by default."""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.db.engine import get_rag_index_session
from src.db.vector_engine import get_vector_session, is_pgvector_available
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_vector import RagChunkVector
from src.services.rag_index_backfill import backfill_identity_rows

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Backfill identity metadata on both indexes when explicitly applied."""
    parser = argparse.ArgumentParser(description="Backfill sparse/vector RAG identity metadata")
    parser.add_argument("--apply", action="store_true", help="Persist changes; dry-run is the default")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    args = parser.parse_args(argv)
    if not is_pgvector_available():
        sys.stderr.write("backfill_error: pgvector is unavailable\n")
        return 2

    with get_rag_index_session() as primary_session, get_vector_session() as vector_session:
        primary_rows = list(primary_session.execute(select(RagChunk)).scalars().all())
        vector_rows = list(vector_session.execute(select(RagChunkVector)).scalars().all())
        primary_report = backfill_identity_rows(primary_rows, apply=args.apply)
        vector_report = backfill_identity_rows(vector_rows, apply=args.apply)
        if args.apply:
            primary_session.commit()
            vector_session.commit()

    payload = {
        "apply": args.apply,
        "primary": primary_report.to_dict(),
        "vector": vector_report.to_dict(),
    }
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(f"apply={args.apply} primary={primary_report.changed} vector={vector_report.changed}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
