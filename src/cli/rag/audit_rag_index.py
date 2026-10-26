"""Audit sparse/vector RAG index identity and freshness alignment."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.db.engine import get_rag_index_session
from src.db.vector_engine import (
    get_vector_session,
    is_oracle_vector_backend,
    is_pgvector_available,
    shares_operational_database,
)
from src.services.rag_index_consistency import (
    IndexConsistencyReport,
    audit_index_sessions,
    audit_single_store_session,
)

#: Exit codes, named because the scheduler job that runs this must tell them
#: apart. ``EXIT_STORE_UNREACHABLE`` means no vector backend answered and the
#: index was never examined; ``EXIT_INCONSISTENT`` means it was reachable and the
#: contents are wrong. Reporting the first as the second sends an operator to
#: rebuild an index on a database that is simply down.
EXIT_OK = 0
EXIT_INCONSISTENT = 1
EXIT_STORE_UNREACHABLE = 2

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


@dataclass(frozen=True, slots=True)
class GateRequirements:
    """What the caller asked the audit to insist on."""

    complete: bool = False
    nonempty: bool = False
    postings: bool = False


def _gate_verdict(
    report: IndexConsistencyReport,
    requirements: GateRequirements,
    *,
    nonempty: bool,
    postings_missing: int | None,
) -> tuple[bool, str | None]:
    """Decide whether the audit passes, and name the first reason it does not.

    The two failure kinds are not the same claim. A store whose rows disagree
    with the vector beside them is wrong now, and no amount of waiting fixes it,
    so it fails whatever the flags say. A store that is still having chunks
    embedded is behind, and only fails when the caller asks for completeness --
    otherwise a draining crawl backlog raises a warning every single day, and a
    warning that is always on is one nobody reads.

    Args:
        report: The consistency report to judge.
        requirements: The flags the caller passed.
        nonempty: Whether both sides hold at least one row.
        postings_missing: Chunks lacking sparse postings, when that was measured.

    Returns:
        Whether the gate passes, and the reason when it does not.

    """
    damage = report.damage_keys
    incomplete = report.incomplete_keys
    if requirements.postings and postings_missing:
        return False, f"{postings_missing} active chunks missing sparse postings"
    if damage:
        return False, f"{len(damage)} identities disagree between the sparse and vector stores"
    if requirements.complete and incomplete:
        return False, f"{len(incomplete)} identities are not fully indexed yet"
    if requirements.nonempty and not nonempty:
        return False, "RAG index is empty"
    return True, None


def main(argv: Sequence[str] | None = None) -> int:
    """Audit sparse and vector indexes without modifying either database."""
    parser = argparse.ArgumentParser(description="Audit sparse/vector RAG index consistency")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument(
        "--require-complete",
        "--fail-on-findings",
        action="store_true",
        dest="require_complete",
        help=(
            "Return exit code 1 when chunks are still missing embeddings or awaiting a reindex, "
            "not only when the sparse and vector stores disagree (--fail-on-findings is the old name)"
        ),
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
        rendered = json.dumps(payload, ensure_ascii=False) if args.as_json else str(payload["error"])
        sys.stdout.write(rendered + "\n")
        return EXIT_STORE_UNREACHABLE

    postings_missing = None
    if is_oracle_vector_backend():
        with get_rag_index_session() as primary_session:
            report = audit_single_store_session(primary_session)
            postings_missing = _count_missing_postings(primary_session)
    elif shares_operational_database():
        # One database holding one ``rag_chunks`` table: load it once. The
        # postings gate stays Oracle-only -- PostgreSQL retrieves through
        # ``content_tsv``, and counting missing terms in the 27.8M-row
        # ``rag_chunk_terms`` table exceeds the scheduler window.
        with get_rag_index_session() as primary_session:
            report = audit_single_store_session(primary_session)
    else:
        with get_rag_index_session() as primary_session, get_vector_session() as vector_session:
            report = audit_index_sessions(primary_session, vector_session)
    payload = report.to_dict()
    if postings_missing is not None:
        payload["postings_missing"] = postings_missing
    consistent, error = _gate_verdict(
        report,
        GateRequirements(
            complete=args.require_complete,
            nonempty=args.require_nonempty,
            postings=args.require_postings,
        ),
        nonempty=report.primary_count > 0 and report.vector_count > 0,
        postings_missing=postings_missing,
    )
    if error is not None:
        payload["error"] = error
    # ``consistent`` is the gate verdict, so it stays aligned with the exit code.
    # Whether the index is *finished* is a different question and is reported
    # separately, because a store can be behind without being wrong.
    payload["fully_consistent"] = report.is_consistent
    payload["consistent"] = consistent
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            f"primary={report.primary_count} vector={report.vector_count} findings={len(report.findings)}\n"
        )
        for finding in report.findings:
            sys.stdout.write(f"{finding.issue}: {finding.source_key}\n")
    return EXIT_INCONSISTENT if not consistent else EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
