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
    args = parser.parse_args(argv)

    if not is_pgvector_available() and not is_oracle_vector_backend():
        payload = {"consistent": False, "error": "pgvector is unavailable; Oracle VECTOR backend is also unavailable"}
        rendered = json.dumps(payload, ensure_ascii=False) if args.as_json else payload["error"]
        sys.stdout.write(rendered + "\n")
        return 2

    if is_oracle_vector_backend():
        with get_rag_index_session() as primary_session:
            report = audit_single_store_session(primary_session)
    else:
        with get_rag_index_session() as primary_session, get_vector_session() as vector_session:
            report = audit_index_sessions(primary_session, vector_session)
    payload = report.to_dict()
    nonempty = report.primary_count > 0 and report.vector_count > 0
    consistent = report.is_consistent and (nonempty or not args.require_nonempty)
    payload["consistent"] = consistent
    if args.require_nonempty and not nonempty:
        payload["error"] = "RAG index is empty"
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
