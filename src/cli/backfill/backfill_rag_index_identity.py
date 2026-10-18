"""Backfill shared RAG index identity metadata in dry-run mode by default.

The four identity columns were added by migration 051 with a ``DEFAULT
'ACTIVE'`` on ``index_status``, so a bulk load that never mentioned them left
583,265 rows reading ``ACTIVE`` by default while ``index_version`` and
``indexed_at`` stayed NULL. ``ACTIVE`` is what the column defaulted to, not an
observation, which is why the lifecycle gates read the whole corpus as healthy
on a column nobody wrote.

Two flags exist because the honest values are not always the defaults:

``--index-version``
    Defaults to ``current_index_version()``. Override it when the corpus was
    produced outside the application pipeline -- stamping the application's
    default onto a foreign corpus makes the incremental selector treat it as
    current and skip re-embedding forever.

``--indexed-at``
    Defaults to now. Override it for a backfill over rows that were embedded on
    a known earlier date; ``now()`` would claim they were embedded today.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import defer

from src.db.engine import get_rag_index_session
from src.db.vector_engine import get_vector_session, is_pgvector_available
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_vector import RagChunkVector
from src.services.rag_index_backfill import backfill_identity_rows
from src.services.rag_index_identity import current_index_version

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from contextlib import AbstractContextManager as ContextManager
    from datetime import datetime

    from sqlalchemy.orm import Session

#: Rows are loaded in batches rather than all at once: a 583k-row table read
#: into a list exhausts memory on a host that is already recovering from an
#: outage.
BATCH_SIZE = 5_000


def _parse_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO timestamp for ``--indexed-at``."""
    if not value:
        return None
    from datetime import datetime

    return datetime.fromisoformat(value)


def _backfill_in_batches(  # noqa: PLR0913
    session: Session,
    model: type[Any],
    *,
    apply: bool,
    index_version: str | None,
    indexed_at: datetime | None,
    defer_columns: Sequence[str] = (),
) -> dict[str, int]:
    """Stream the table in batches, filling only rows that are missing identity.

    Args:
        session: Session bound to the index being backfilled.
        model: ORM model carrying the identity columns.
        apply: Persist instead of previewing.
        index_version: Version to stamp; the configured default when absent.
        indexed_at: Timestamp to stamp; now when absent.
        defer_columns: Column names to leave out of the SELECT. The identity
            backfill touches only the identity columns, so pulling a large
            embedding column for 583k rows is wasted transfer. More
            importantly, the primary index has no ``embedding_vector`` column at
            all -- it is Oracle-only, PostgreSQL keeps JSON in ``embedding`` --
            and a bare ``select(model)`` names every mapped column, so the pass
            dies with ``UndefinedColumn`` before reading a single row.
            ``--skip-vector`` skips the *vector store*; nothing else kept the
            primary pass from failing on the store that owns the sparse rows.

    Returns:
        The aggregate report.

    """
    from src.services.rag_index_backfill import IdentityBackfillReport

    # Accumulators are plain ints: the report dataclass is frozen, and a backfill
    # that dies on its own bookkeeping after the first batch is a wasted write.
    scanned = 0
    changed = 0
    missing_source = 0
    # Keyset pagination, not OFFSET. OFFSET without a stable ORDER BY has no
    # guaranteed row order, so a 583k-row pass can skip a batch or repeat one --
    # and a backfill that silently covers a third of the corpus while reporting
    # every row as scanned is worse than one that fails.
    last_id = 0
    deferred = [getattr(model, name) for name in defer_columns]
    while True:
        query = select(model).where(model.id > last_id).order_by(model.id).limit(BATCH_SIZE)
        if deferred:
            query = query.options(defer(*deferred))
        rows = list(
            session.execute(query).scalars().all(),
        )
        if not rows:
            break
        batch = backfill_identity_rows(rows, apply=apply, index_version=index_version, indexed_at=indexed_at)
        scanned += batch.scanned
        changed += batch.changed
        missing_source += batch.missing_source
        if apply:
            session.flush()
        last_id = rows[-1].id
    return IdentityBackfillReport(scanned, changed, missing_source).to_dict()


def main(argv: Sequence[str] | None = None) -> int:
    """Backfill identity metadata on both indexes when explicitly applied.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="Persist changes; dry-run is the default")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument(
        "--index-version",
        default=None,
        help=f"Version to stamp (default: configured {current_index_version()!r})",
    )
    parser.add_argument(
        "--indexed-at",
        default=None,
        help="ISO timestamp to stamp (default: now)",
    )
    parser.add_argument(
        "--skip-vector",
        action="store_true",
        help="Backfill only the primary index, for hosts without pgvector configured",
    )
    args = parser.parse_args(argv)

    indexed_at = _parse_timestamp(args.indexed_at)
    if not args.skip_vector and not is_pgvector_available():
        sys.stderr.write("backfill_error: pgvector is unavailable; pass --skip-vector to run primary only\n")
        return 2

    primary_payload = _run(
        get_rag_index_session,
        RagChunk,
        apply=args.apply,
        index_version=args.index_version,
        indexed_at=indexed_at,
        # ORM attribute name, not the DB column name. The identity backfill never
        # reads it, and the primary store does not have the Oracle-only
        # `embedding_vector` column that this attribute maps to.
        defer_columns=("embedding",),
    )
    vector_payload = (
        None
        if args.skip_vector
        else _run(
            get_vector_session,
            RagChunkVector,
            apply=args.apply,
            index_version=args.index_version,
            indexed_at=indexed_at,
        )
    )

    payload = {
        "apply": args.apply,
        "index_version": args.index_version or current_index_version(),
        "indexed_at": args.indexed_at,
        "primary": primary_payload,
        "vector": vector_payload,
    }
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        primary_changed = primary_payload["changed"] if primary_payload else 0
        vector_changed = vector_payload["changed"] if vector_payload else 0
        version = payload["index_version"]
        sys.stdout.write(f"apply={args.apply} version={version} primary={primary_changed} vector={vector_changed}\n")
    return 0


def _run(  # noqa: PLR0913
    session_factory: Callable[[], ContextManager[Session]],
    model: type[Any],
    *,
    apply: bool,
    index_version: str | None,
    indexed_at: datetime | None,
    defer_columns: Sequence[str] = (),
) -> dict[str, int]:
    """Backfill one index and commit it when applying.

    Args:
        session_factory: Context manager yielding a session.
        model: ORM model carrying the identity columns.
        apply: Persist instead of previewing.
        index_version: Version to stamp.
        indexed_at: Timestamp to stamp.
        defer_columns: Column names to leave out of the SELECT; see
            ``_backfill_in_batches``.

    Returns:
        The aggregate report.

    """
    with session_factory() as session:
        payload = _backfill_in_batches(
            session,
            model,
            apply=apply,
            index_version=index_version,
            indexed_at=indexed_at,
            defer_columns=defer_columns,
        )
        if apply:
            session.commit()
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
