"""Fill the RAG identity columns that migration 051 added without values.

``051_rag_index_consistency.sql`` created ``content_hash`` and ``index_version``
as nullable columns and backfilled only ``index_status``. Rows that a later
UPSERT never touched therefore kept NULLs, and ``audit_rag_index`` reports each
NULL as a finding, so the sentinel gate stays red for a store that has no real
divergence.

Both values are deterministic and already owned by the UPSERT path
(``chunk_content_hash`` and ``current_index_version``), so this is a recovery of
missing values rather than a judgment about content. Stored values are never
overwritten: a differing hash means the chunk may be stale, which is a
re-indexing decision, not a backfill one. The sample below only reports it.

Usage:
    python3 -m scripts.maintenance.backfill_rag_index_identity --dry-run
    python3 -m scripts.maintenance.backfill_rag_index_identity --apply
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update

from src.db.engine import SessionLocal
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import chunk_content_hash, current_index_version

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000
CONTRACT_SAMPLE = 20


@dataclass(frozen=True)
class BackfillReport:
    """What the backfill found and what it wrote."""

    content_hash_targets: int = 0
    index_version_targets: int = 0
    content_hash_written: int = 0
    index_version_written: int = 0
    contract_sample: int = 0
    contract_violations: int = 0


def _count_missing(session: Session, column: object) -> int:
    """Count rows whose identity column is still NULL."""
    statement = select(func.count()).select_from(RagChunk).where(column.is_(None))
    return int(session.scalar(statement) or 0)


def _sample_contract(session: Session, limit: int) -> tuple[int, int]:
    """Return how many sampled stored hashes the canonical function reproduces.

    A violation is not a backfill failure -- those rows already carry a value
    and are out of scope -- but an all-violating sample means the hashing
    contract itself moved and the backfill would write values nothing agrees
    with. The caller reports both numbers so that reading is possible.
    """
    if limit <= 0:
        return 0, 0
    rows = session.execute(
        select(RagChunk.id, RagChunk.title, RagChunk.content, RagChunk.content_hash)
        .where(RagChunk.content_hash.is_not(None))
        .limit(limit),
    ).all()
    violations = sum(1 for row in rows if chunk_content_hash(row[1], row[2]) != row[3])
    return len(rows), violations


def _write_index_version(session: Session) -> int:
    """Fill every NULL ``index_version`` in one statement."""
    result = session.execute(
        update(RagChunk).where(RagChunk.index_version.is_(None)).values(index_version=current_index_version()),
    )
    return int(result.rowcount or 0)


def _write_content_hash(session: Session, *, batch_size: int) -> int:
    """Fill NULL ``content_hash`` values batch by batch.

    Keyset pagination on the primary key keeps each batch an index range scan.
    ``OFFSET`` would re-walk everything already filled and get slower the longer
    the backfill runs.
    """
    written = 0
    last_id = 0
    while True:
        rows = session.execute(
            select(RagChunk.id, RagChunk.title, RagChunk.content)
            .where(RagChunk.content_hash.is_(None), RagChunk.id > last_id)
            .order_by(RagChunk.id)
            .limit(batch_size),
        ).all()
        if not rows:
            return written
        session.execute(
            update(RagChunk),
            [{"id": row[0], "content_hash": chunk_content_hash(row[1], row[2])} for row in rows],
        )
        written += len(rows)
        last_id = rows[-1][0]
        logger.info("content_hash backfill: %s rows written (last id=%s)", written, last_id)


def backfill_rag_index_identity(
    session: Session,
    *,
    apply: bool,
    batch_size: int = BATCH_SIZE,
    contract_sample: int = CONTRACT_SAMPLE,
) -> BackfillReport:
    """Report the missing identity values and, when ``apply``, fill them."""
    content_hash_targets = _count_missing(session, RagChunk.content_hash)
    index_version_targets = _count_missing(session, RagChunk.index_version)
    sampled, violations = _sample_contract(session, contract_sample)

    content_hash_written = 0
    index_version_written = 0
    if apply:
        index_version_written = _write_index_version(session)
        content_hash_written = _write_content_hash(session, batch_size=batch_size)

    return BackfillReport(
        content_hash_targets=content_hash_targets,
        index_version_targets=index_version_targets,
        content_hash_written=content_hash_written,
        index_version_written=index_version_written,
        contract_sample=sampled,
        contract_violations=violations,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the backfill from the command line."""
    parser = argparse.ArgumentParser(description="Backfill NULL RAG index identity columns")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report targets only; this is the default")
    mode.add_argument("--apply", action="store_true", help="Write the missing values")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Rows per content_hash write batch")
    parser.add_argument("--contract-sample", type=int, default=CONTRACT_SAMPLE, help="Stored hashes to check")
    args = parser.parse_args(argv)

    with SessionLocal() as session:
        report = backfill_rag_index_identity(
            session,
            apply=args.apply,
            batch_size=args.batch_size,
            contract_sample=args.contract_sample,
        )
        if args.apply:
            session.commit()

    mode = "APPLIED" if args.apply else "DRY RUN"
    sys.stdout.write(
        f"[{mode}] content_hash={report.content_hash_written}/{report.content_hash_targets} "
        f"index_version={report.index_version_written}/{report.index_version_targets} "
        f"contract_sample={report.contract_sample} violations={report.contract_violations}\n",
    )
    if not args.apply:
        sys.stdout.write("Re-run with --apply to write the missing values.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
