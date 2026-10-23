"""Vector store tombstone reactivation helpers for RAG chunk upserts.

When maintenance or cleanup scripts mark corrupted or obsolete RAG entries as
DELETED, the vector store (pgvector) sets tombstone markers:
    is_active = false
    valid_to = COALESCE(valid_to, now())
In addition, the cleanup script stamps a tombstone marker into the chunk's JSON
metadata:
    CLEANUP_TOMBSTONE_META_KEY = "kbo_cleanup_tombstoned" (meta[key] = true)

Why the marker is needed:
During normal publishing (both the bulk PostgreSQL path `_publish_bulk` and the
normal ORM path `publish_index_batch`), RAG chunks transition through a
`PENDING` state before reaching `ACTIVE`. Consequently, any inspection checking
whether the "previous status was DELETED" immediately prior to the `ACTIVE`
write fails to trigger: the row was already updated to `PENDING`. Without a
persistent marker, cleaned up rows that are recrawled and published would remain
stuck in `ACTIVE | is_active = false | valid_to set` (hidden from downstream
retrievers such as bega_AI) forever.

Design highlights:
- Persistent marker in the vector DB: `meta["kbo_cleanup_tombstoned"] = True`
  is set atomically during cleanup on affected vector rows.
- Works without external schema changes (e.g. bega_AI #31), operating strictly
  within existing physical columns (`meta`, `is_active`, `valid_to`).
- Atomic preservation: In PostgreSQL bulk upserts (`_bulk_upsert_vector`), the
  `ON CONFLICT` update preserves the marker atomically via a SQL `CASE`
  expression on `rag_chunks.meta`, preventing race conditions without read-then-write.
- Atomic reactivation: On the final `ACTIVE` upsert, a single conditional
  `UPDATE` clears `is_active = true`, `valid_to = NULL`, and strips the marker
  from `meta` in the same transaction before commit.
- Curated & time-bounded rows preserved: Chunks never touched by the cleanup
  script lack the marker, ensuring that intentionally deactivated or time-bounded
  `is_active = false` rows are never reactivated.
- Sparse index independence: Sparse KBO `rag_chunks` has no `is_active` or
  `valid_to` columns, so only the vector store requires this tombstone lifecycle.
"""

from __future__ import annotations

import contextlib
import logging
import weakref
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, inspect, text
from sqlalchemy.exc import NoInspectionAvailable, NoSuchTableError, SQLAlchemyError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CLEANUP_TOMBSTONE_META_KEY: str = "kbo_cleanup_tombstoned"

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


def reactivate_cleanup_tombstoned_keys(session: Session, keys: Sequence[tuple[str, str]]) -> int:
    """Clear cleanup tombstone marker and restore activity for reactivated vector chunks.

    Runs after the ACTIVE upsert and before commit to atomically re-activate rows
    marked with the cleanup tombstone marker (CLEANUP_TOMBSTONE_META_KEY).
    Only existing columns among is_active and valid_to are updated; the marker is
    always stripped from the meta JSON.

    Args:
        session: Active database session.
        keys: Sequence of (source_table, source_row_id) pairs to reactivate.

    Returns:
        Number of rows reactivated.

    """
    if not keys:
        return 0

    target_cols = get_reactivation_columns(session)
    set_clauses: list[str] = []
    if "is_active" in target_cols:
        set_clauses.append("is_active = true")
    if "valid_to" in target_cols:
        set_clauses.append("valid_to = NULL")

    dialect_name = ""
    try:
        bind = session.get_bind()
        dialect_name = str(getattr(getattr(bind, "dialect", None), "name", ""))
    except TypeError, SQLAlchemyError:
        dialect_name = ""

    if dialect_name == "postgresql":
        set_clauses.append(f"meta = meta - '{CLEANUP_TOMBSTONE_META_KEY}'")
        where_cond = f"meta ? '{CLEANUP_TOMBSTONE_META_KEY}'"
    else:
        set_clauses.append(f"meta = json_remove(meta, '$.{CLEANUP_TOMBSTONE_META_KEY}')")
        where_cond = f"json_extract(meta, '$.{CLEANUP_TOMBSTONE_META_KEY}') IS NOT NULL"

    key_list = [(str(t), str(r)) for t, r in keys]
    stmt = text(
        f"UPDATE rag_chunks SET {', '.join(set_clauses)} "  # noqa: S608
        f"WHERE (source_table, source_row_id) IN :keys "
        f"AND index_status = 'ACTIVE' "
        f"AND {where_cond}"
    ).bindparams(bindparam("keys", expanding=True))

    res = session.execute(stmt, {"keys": key_list})
    return int(res.rowcount or 0)
