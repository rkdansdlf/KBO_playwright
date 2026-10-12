"""Tombstone chunks in the vector store, mirroring ``tombstone_rag_chunks``.

The primary-side tool sets ``index_status = DELETED`` on the sparse store and says
nothing about the vector store, because until the vectors moved to pgvector there
was no other copy to keep quiet. Now a chunk lives in both places carrying the
same identity, so a tombstone applied to one of them leaves the other serving the
deleted content: the dense leg still returns it, and the reconciliation gate
reports an index_status mismatch for every key the sparse side retired.

Only retired rows are touched. A key that is already DELETED, or absent, is
reported as such rather than counted as work done -- an idempotent run of this
tool must not look like a productive one.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VectorTombstoneResult:
    """Outcome for one source key."""

    source_key: str
    found: bool
    previous_status: str
    final_status: str
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable rendering."""
        return {
            "source_key": self.source_key,
            "found": self.found,
            "previous_status": self.previous_status,
            "final_status": self.final_status,
            "error": self.error,
        }


_TIE_KEY_HASH = "01VECTOR_TOMBSTONE_TIE_KEY"


def split_source_key(source_key: str) -> tuple[str, str]:
    """Split ``table:row_id`` into its parts, rejecting anything else."""
    source_table, separator, source_row_id = source_key.partition(":")
    if not separator or not source_table or not source_row_id:
        message = f"expected 'table:row_id', got {source_key!r}"
        raise ValueError(message)
    return source_table, source_row_id


def inspect_vector_tombstones(session: Session, source_keys: Sequence[str]) -> tuple[VectorTombstoneResult, ...]:
    """Report the current state of the requested rows without writing."""
    return tuple(_inspect_one(session, key) for key in source_keys)


def tombstone_vector_rows(session: Session, source_keys: Sequence[str]) -> tuple[VectorTombstoneResult, ...]:
    """Mark the requested rows DELETED in the vector store."""
    results = tuple(_apply_one(session, key) for key in source_keys)
    session.commit()
    return results


def _inspect_one(session: Session, source_key: str) -> VectorTombstoneResult:
    """Read one row's current state."""
    try:
        source_table, source_row_id = split_source_key(source_key)
    except ValueError as error:
        return VectorTombstoneResult(source_key, found=False, previous_status="", final_status="", error=str(error))
    statement = text("SELECT index_status FROM rag_chunks WHERE source_table = :table AND source_row_id = :row_id")
    row = session.execute(statement, {"table": source_table, "row_id": source_row_id}).first()
    if row is None:
        return VectorTombstoneResult(source_key, found=False, previous_status="", final_status="")
    status = str(row[0] or "")
    return VectorTombstoneResult(source_key, found=True, previous_status=status, final_status=status)


def _apply_one(session: Session, source_key: str) -> VectorTombstoneResult:
    """Retire one row, reporting the state it came from."""
    current = _inspect_one(session, source_key)
    if current.error is not None or not current.found:
        return current
    if current.previous_status == "DELETED":
        return current
    source_table, source_row_id = split_source_key(source_key)
    statement = text(
        "UPDATE rag_chunks SET index_status = 'DELETED' "
        "WHERE source_table = :table AND source_row_id = :row_id AND index_status <> 'DELETED'"
    )
    try:
        session.execute(statement, {"table": source_table, "row_id": source_row_id})
    except SQLAlchemyError as error:
        session.rollback()
        logger.exception("Vector tombstone failed for %s", source_key)
        return VectorTombstoneResult(
            source_key, found=True, previous_status=current.previous_status, final_status="ACTIVE", error=str(error)
        )
    return VectorTombstoneResult(
        source_key, found=True, previous_status=current.previous_status, final_status="DELETED"
    )
