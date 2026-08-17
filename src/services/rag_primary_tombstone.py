"""Primary-index-only tombstone operations used before vector provisioning."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.rag_chunk import RagChunk
from src.services.rag_index_lifecycle import transition_status

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class PrimaryTombstoneResult:
    """Describe one sparse-index tombstone operation."""

    source_key: str
    previous_status: str | None
    final_status: str
    found: bool
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize a tombstone result."""
        return {
            "source_key": self.source_key,
            "previous_status": self.previous_status,
            "final_status": self.final_status,
            "found": self.found,
            "error": self.error,
        }


def inspect_primary_tombstones(session: Session, source_keys: tuple[str, ...]) -> tuple[PrimaryTombstoneResult, ...]:
    """Read current primary lifecycle states without mutating rows."""
    results: list[PrimaryTombstoneResult] = []
    for source_key in source_keys:
        source_table, source_row_id = _split_source_key(source_key)
        row = session.scalar(
            select(RagChunk).where(
                RagChunk.source_table == source_table,
                RagChunk.source_row_id == source_row_id,
            ),
        )
        if row is None:
            results.append(PrimaryTombstoneResult(source_key, None, "MISSING", found=False))
            continue
        status = row.index_status or "ACTIVE"
        results.append(PrimaryTombstoneResult(source_key, status, status, found=True))
    return tuple(results)


def tombstone_primary_rows(session: Session, source_keys: tuple[str, ...]) -> tuple[PrimaryTombstoneResult, ...]:
    """Mark primary rows DELETE_PENDING, then DELETED, without vector mutation."""
    rows: list[tuple[str, RagChunk, str]] = []
    results: list[PrimaryTombstoneResult] = []
    for source_key in source_keys:
        source_table, source_row_id = _split_source_key(source_key)
        row = session.scalar(
            select(RagChunk).where(
                RagChunk.source_table == source_table,
                RagChunk.source_row_id == source_row_id,
            ),
        )
        if row is None:
            results.append(PrimaryTombstoneResult(source_key, None, "MISSING", found=False))
            continue
        previous_status = row.index_status or "ACTIVE"
        if previous_status in {"DELETED", "TOMBSTONED"}:
            results.append(PrimaryTombstoneResult(source_key, previous_status, previous_status, found=True))
            continue
        try:
            row.index_status = transition_status(previous_status, "DELETE_PENDING")
        except ValueError:
            results.append(
                PrimaryTombstoneResult(
                    source_key,
                    previous_status,
                    previous_status,
                    found=True,
                    error="invalid lifecycle",
                ),
            )
            continue
        rows.append((source_key, row, previous_status))

    session.commit()
    for source_key, row, previous_status in rows:
        row.index_status = transition_status(row.index_status, "DELETED")
        results.append(PrimaryTombstoneResult(source_key, previous_status, "DELETED", found=True))
    session.commit()
    return tuple(results)


def _split_source_key(source_key: str) -> tuple[str, str]:
    """Split a canonical source key at its first separator."""
    source_table, separator, source_row_id = source_key.partition(":")
    if not separator or not source_table or not source_row_id:
        error_message = f"Invalid RAG source key: {source_key}"
        raise ValueError(error_message)
    return source_table, source_row_id
