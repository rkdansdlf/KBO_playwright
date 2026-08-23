"""Build the Oracle sparse postings index for canonical RAG chunks."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from sqlalchemy import delete, func, insert, select
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import get_rag_index_session
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_term import RagChunkTerm
from src.services.rag_index_identity import RETRIEVABLE_INDEX_STATUSES
from src.services.rag_sparse_terms import build_term_rows

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
DEFAULT_BATCH_SIZE = 500


@dataclass(frozen=True, slots=True)
class SparseTermBuildReport:
    """Summarize one sparse postings build or dry-run."""

    chunks_scanned: int
    chunks_with_terms: int
    term_rows: int
    dry_run: bool
    rebuilt: bool

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-compatible report."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SparseTermBuildOptions:
    """Configure one sparse postings build or resume."""

    apply: bool
    rebuild: bool
    limit: int | None = None
    batch_size: int = DEFAULT_BATCH_SIZE
    after_id: int = 0


def _iter_chunk_batches(
    session: Session,
    limit: int | None,
    batch_size: int,
    after_id: int = 0,
) -> Iterator[list[tuple[int, str, str | None, str, object]]]:
    """Yield bounded active-chunk pages without holding a streaming cursor open."""
    chunks_scanned = 0
    last_id = after_id
    while limit is None or chunks_scanned < limit:
        page_size = batch_size if limit is None else min(batch_size, limit - chunks_scanned)
        stmt = (
            select(RagChunk.id, RagChunk.source_table, RagChunk.title, RagChunk.content, RagChunk.meta)
            .where(
                RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
                RagChunk.id > last_id,
            )
            .order_by(RagChunk.id)
            .limit(page_size)
        )
        rows = session.execute(stmt).all()
        if not rows:
            return
        batch = [(row.id, row.source_table, row.title, row.content, row.meta) for row in rows]
        yield batch
        chunks_scanned += len(batch)
        last_id = batch[-1][0]


def _build_batch_postings(batch: list[tuple[int, str, str | None, str, object]]) -> list[dict[str, object]]:
    """Convert one chunk batch into sparse posting mappings."""
    postings: list[dict[str, object]] = []
    for rag_chunk_id, source_table, title, content, meta in batch:
        metadata = meta if isinstance(meta, dict) else None
        postings.extend(build_term_rows(rag_chunk_id, title, content, metadata, source_table=source_table))
    return postings


def _validate_build_options(options: SparseTermBuildOptions) -> None:
    """Validate mutually exclusive and bounded build options."""
    if options.batch_size < 1:
        message = "batch-size must be positive"
        raise ValueError(message)
    if options.rebuild and not options.apply:
        message = "--rebuild requires --apply"
        raise ValueError(message)
    if options.rebuild and options.limit is not None:
        message = "--rebuild cannot be combined with --limit"
        raise ValueError(message)
    if options.rebuild and options.after_id:
        message = "--rebuild cannot be combined with --after-id"
        raise ValueError(message)
    if options.after_id < 0:
        message = "after-id must not be negative"
        raise ValueError(message)


def _build_sparse_terms(session: Session, options: SparseTermBuildOptions) -> SparseTermBuildReport:
    """Build or preview Oracle sparse postings for active RAG chunks."""
    _validate_build_options(options)

    if options.apply and options.rebuild:
        session.execute(delete(RagChunkTerm))
        session.commit()

    chunks_scanned = 0
    chunks_with_terms = 0
    term_rows = 0
    for batch in _iter_chunk_batches(session, options.limit, options.batch_size, options.after_id):
        postings = _build_batch_postings(batch)
        chunks_scanned += len(batch)
        chunks_with_terms += len({int(row["rag_chunk_id"]) for row in postings})
        term_rows += len(postings)

        if not options.apply:
            continue
        chunk_ids = [chunk[0] for chunk in batch]
        # Resume ranges start after the highest committed chunk, so they are
        # insert-only and avoid Oracle delete-then-reinsert row-lock cycles.
        if not options.rebuild and not options.after_id:
            session.execute(delete(RagChunkTerm).where(RagChunkTerm.rag_chunk_id.in_(chunk_ids)))
        if postings:
            session.execute(insert(RagChunkTerm), postings)
        session.commit()
        logger.info("Sparse postings: chunks=%d terms=%d", chunks_scanned, term_rows)

    return SparseTermBuildReport(
        chunks_scanned=chunks_scanned,
        chunks_with_terms=chunks_with_terms,
        term_rows=term_rows,
        dry_run=not options.apply,
        rebuilt=options.rebuild,
    )


def build_sparse_terms(
    session: Session,
    *,
    apply: bool,
    rebuild: bool,
    limit: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> SparseTermBuildReport:
    """Build or preview Oracle sparse postings for active RAG chunks."""
    return _build_sparse_terms(
        session,
        SparseTermBuildOptions(
            apply=apply,
            rebuild=rebuild,
            limit=limit,
            batch_size=batch_size,
        ),
    )


def _apply_guard() -> None:
    """Require explicit Oracle write approval for a postings build."""
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url.startswith("oracle"):
        message = "sparse term apply requires an Oracle DATABASE_URL"
        raise ValueError(message)
    if os.getenv("RAG_INDEX_ALLOW_WRITE") != "1":
        message = "sparse term apply requires RAG_INDEX_ALLOW_WRITE=1"
        raise ValueError(message)
    target_environment = os.getenv("RAG_TARGET_ENV", "").strip().lower()
    if target_environment not in {"staging", "production"}:
        message = "sparse term apply requires RAG_TARGET_ENV=staging or production"
        raise ValueError(message)
    if target_environment == "production" and os.getenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE") != "1":
        message = "production sparse term apply requires RAG_INDEX_ALLOW_PRODUCTION_WRITE=1"
        raise ValueError(message)


def max_posted_chunk_id(session: Session) -> int:
    """Return the highest chunk ID that already has sparse postings."""
    return int(session.scalar(select(func.max(RagChunkTerm.rag_chunk_id))) or 0)


def main(argv: Sequence[str] | None = None) -> int:
    """Build or preview the Oracle RAG sparse postings index."""
    parser = argparse.ArgumentParser(description="Build Oracle RAG sparse term postings")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Write postings; dry-run is the default")
    mode.add_argument("--dry-run", action="store_true", help="Preview postings without writing")
    parser.add_argument("--rebuild", action="store_true", help="Delete all postings before rebuilding")
    parser.add_argument("--limit", type=int, default=None, help="Process at most this many chunks")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Chunks per database batch")
    parser.add_argument("--after-id", type=int, default=0, help="Resume after this RAG chunk ID")
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help="Resume insert-only after the highest already-indexed chunk",
    )
    parser.add_argument("--json", action="store_true", help="Render a JSON report")
    args = parser.parse_args(argv)

    if args.catch_up and args.rebuild:
        sys.stderr.write("--catch-up cannot be combined with --rebuild\n")
        return 2
    if args.catch_up and args.after_id:
        sys.stderr.write("--catch-up cannot be combined with --after-id\n")
        return 2

    try:
        if args.apply:
            _apply_guard()
        with get_rag_index_session() as session:
            after_id = args.after_id
            if args.catch_up:
                after_id = max_posted_chunk_id(session)
                logger.info("Sparse catch-up resumes after chunk %d", after_id)
            report = _build_sparse_terms(
                session,
                SparseTermBuildOptions(
                    apply=args.apply,
                    rebuild=args.rebuild,
                    limit=args.limit,
                    batch_size=args.batch_size,
                    after_id=after_id,
                ),
            )
    except (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError):
        logger.exception("Oracle sparse term build failed")
        return 1

    rendered = report.as_dict()
    if args.json:
        sys.stdout.write(json.dumps(rendered, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            "chunks={chunks_scanned} chunks_with_terms={chunks_with_terms} terms={term_rows} "
            "dry_run={dry_run} rebuilt={rebuilt}\n".format(**rendered),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
