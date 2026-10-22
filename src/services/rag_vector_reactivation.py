"""Vector store tombstone reactivation helpers for RAG chunk upserts.

When maintenance or cleanup scripts mark corrupted or obsolete RAG entries as
DELETED, external schemas (such as bega_AI) set tombstone markers:
    is_active = false
    valid_to = COALESCE(valid_to, now())
Because KBO's RagChunkVector ORM model does not map is_active or valid_to,
subsequent upserts that restore a chunk to ACTIVE only write index_status,
leaving the tombstone flags untouched. Downstream consumers filtering on
is_active and valid_to would therefore continue to treat the row as deleted.

The functions in this module ensure that legitimately re-indexed chunks
(whose index_status was 'DELETED' immediately prior to the upsert and is now
'ACTIVE') have their tombstone markers cleared:
    is_active = true
    valid_to = NULL

Crucially, this reactivation is ONLY performed when transitioning from
'DELETED' to 'ACTIVE'. Rows that were already 'ACTIVE' with an intentional
time-bound or curated valid_to / is_active flag are never touched.
"""

from __future__ import annotations

import contextlib
import logging
import weakref
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy import inspect, select, text, tuple_
from sqlalchemy.exc import NoInspectionAvailable, NoSuchTableError, SQLAlchemyError

from src.models.rag_chunk_vector import RagChunkVector

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

_COLUMNS_CACHE: weakref.WeakKeyDictionary[Any, set[str]] = weakref.WeakKeyDictionary()
_TOMBSTONE_COLUMNS = frozenset({"is_active", "valid_to"})


def clear_reactivation_columns_cache() -> None:
    """Clear the cached table column inspection per bind (for tests)."""
    _COLUMNS_CACHE.clear()


def get_reactivation_columns(session: Session) -> set[str]:
    """Inspect and return the subset of tombstone columns present in rag_chunks.

    Args:
        session: Active database session.

    Returns:
        Set of matching column names present in the physical table
        (subset of {'is_active', 'valid_to'}).

    """
    try:
        bind = session.get_bind()
        if bind in _COLUMNS_CACHE:
            return _COLUMNS_CACHE[bind]
    except TypeError, SQLAlchemyError:
        bind = None

    try:
        conn = session.connection()
        inspector = inspect(conn)
        columns = {col["name"] for col in inspector.get_columns("rag_chunks")}
        reactivation_cols = columns.intersection(_TOMBSTONE_COLUMNS)
    except (SQLAlchemyError, NoSuchTableError, NoInspectionAvailable) as exc:
        logger.debug("Could not inspect rag_chunks columns for reactivation: %s", exc)
        reactivation_cols = set()

    if bind is not None:
        with contextlib.suppress(TypeError):
            _COLUMNS_CACHE[bind] = reactivation_cols

    return reactivation_cols


def reactivate_vector_chunk_by_id(session: Session, chunk_id: int) -> None:
    """Clear tombstone markers for a reactivated vector chunk by its primary key ID.

    If a row with index_status='DELETED' is being updated to 'ACTIVE', this clears
    'is_active = true' and 'valid_to = NULL' for that row, provided those columns
    exist in the underlying rag_chunks table.

    Args:
        session: Active database session.
        chunk_id: Primary key id of the rag_chunks row.

    """
    target_cols = get_reactivation_columns(session)
    if not target_cols:
        return

    if "is_active" in target_cols and "valid_to" in target_cols:
        stmt = text("UPDATE rag_chunks SET is_active = true, valid_to = NULL WHERE id = :id")
    elif "is_active" in target_cols:
        stmt = text("UPDATE rag_chunks SET is_active = true WHERE id = :id")
    else:
        stmt = text("UPDATE rag_chunks SET valid_to = NULL WHERE id = :id")

    session.execute(stmt, {"id": chunk_id})


def find_deleted_vector_keys(session: Session, keys: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
    """Find source keys among the provided keys currently marked as DELETED in rag_chunks.

    Args:
        session: Active database session.
        keys: Sequence of (source_table, source_row_id) pairs to inspect.

    Returns:
        List of (source_table, source_row_id) tuples whose index_status is 'DELETED'.

    """
    if not keys:
        return []

    key_list = [(str(t), str(r)) for t, r in keys]
    stmt = select(RagChunkVector.source_table, RagChunkVector.source_row_id).where(
        RagChunkVector.index_status == "DELETED",
        tuple_(RagChunkVector.source_table, RagChunkVector.source_row_id).in_(key_list),
    )
    # Errors propagate: swallowing them would leave a PostgreSQL transaction aborted
    # and silently skip the tombstone reset for re-activated chunks.
    rows = session.execute(stmt).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def reactivate_vector_chunks_by_keys(session: Session, keys: Sequence[tuple[str, str]]) -> None:
    """Clear tombstone markers for reactivated vector chunks identified by source keys.

    If rows with index_status='DELETED' are being updated to 'ACTIVE', this clears
    'is_active = true' and 'valid_to = NULL' for those identities, provided those
    columns exist in the underlying rag_chunks table.

    Args:
        session: Active database session.
        keys: Sequence of (source_table, source_row_id) pairs to reactivate.

    """
    if not keys:
        return

    target_cols = get_reactivation_columns(session)
    if not target_cols:
        return

    table = sa.table(
        "rag_chunks",
        sa.column("source_table", sa.Text),
        sa.column("source_row_id", sa.Text),
        sa.column("is_active", sa.Boolean),
        sa.column("valid_to", sa.DateTime),
    )
    values: dict[str, Any] = {}
    if "is_active" in target_cols:
        values["is_active"] = True
    if "valid_to" in target_cols:
        values["valid_to"] = None

    key_list = [(str(t), str(r)) for t, r in keys]
    stmt = sa.update(table).where(tuple_(table.c.source_table, table.c.source_row_id).in_(key_list)).values(**values)
    session.execute(stmt)
