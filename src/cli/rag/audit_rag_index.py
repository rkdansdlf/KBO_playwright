"""Audit sparse/vector RAG index identity and freshness alignment."""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.db.engine import get_rag_index_session
from src.db.vector_engine import get_vector_session, is_oracle_vector_backend, is_pgvector_available
from src.services.rag_index_consistency import audit_index_sessions, audit_single_store_session

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session


def _count_missing_postings(session: Session) -> int:
    """Count retrievable chunks that have no sparse postings row."""
    from sqlalchemy import func, select

    from src.models.rag_chunk import RagChunk
    from src.models.rag_chunk_term import RagChunkTerm
    from src.services.rag_index_identity import RETRIEVABLE_INDEX_STATUSES

    missing_chunks = (
        select(RagChunk.id)
        .where(
            RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
            ~select(RagChunkTerm.rag_chunk_id).where(RagChunkTerm.rag_chunk_id == RagChunk.id).exists(),
        )
        .subquery()
    )
    return int(session.scalar(select(func.count()).select_from(missing_chunks)) or 0)


def main(argv: Sequence[str] | None = None) -> int:
    """Audit sparse and vector indexes without modifying either database."""
    parser = argparse.ArgumentParser(description="Audit sparse/vector RAG index consistency")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument(
        "--fail-on-findings",
        action="store_true",
        help="Compatibility flag; findings always return exit code 1",
    )
    parser.add_argument(
        "--require-nonempty",
        action="store_true",
        help="Return exit code 1 when either index has zero rows",
    )
    parser.add_argument(
        "--require-postings",
        action="store_true",
        help="Return exit code 1 when retrievable chunks lack sparse postings",
    )
    args = parser.parse_args(argv)

    if not is_pgvector_available() and not is_oracle_vector_backend():
        payload = {"consistent": False, "error": "pgvector is unavailable; Oracle VECTOR backend is also unavailable"}
        rendered = json.dumps(payload, ensure_ascii=False) if args.as_json else payload["error"]
        sys.stdout.write(rendered + "\n")
        return 2

    postings_missing = None
    if is_oracle_vector_backend():
        with get_rag_index_session() as primary_session:
            report = audit_single_store_session(primary_session)
            postings_missing = _count_missing_postings(primary_session)
    else:
        with get_rag_index_session() as primary_session, get_vector_session() as vector_session:
            report = audit_index_sessions(primary_session, vector_session)
    payload = report.to_dict()
    if postings_missing is not None:
        payload["postings_missing"] = postings_missing
    nonempty = report.primary_count > 0 and report.vector_count > 0
    consistent = report.is_consistent and (nonempty or not args.require_nonempty)
    postings_gap = args.require_postings and postings_missing is not None and postings_missing > 0
    if postings_gap:
        consistent = False
        payload["error"] = f"{postings_missing} active chunks missing sparse postings"
    elif args.require_nonempty and not nonempty:
        payload["error"] = "RAG index is empty"
    payload["consistent"] = consistent
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            f"primary={report.primary_count} vector={report.vector_count} findings={len(report.findings)}\n"
        )
        for finding in report.findings:
            sys.stdout.write(f"{finding.issue}: {finding.source_key}\n")
    return 1 if not consistent else 0


if __name__ == "__main__":
    raise SystemExit(main())
