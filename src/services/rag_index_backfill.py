"""Backfill shared sparse/vector RAG identity metadata."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from src.constants import KST
from src.services.rag_index_identity import ACTIVE_INDEX_STATUS, chunk_content_hash, current_index_version

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class IdentityBackfillReport:
    """Summarize identity fields filled on one index."""

    scanned: int
    changed: int
    missing_source: int

    def to_dict(self) -> dict[str, int]:
        """Serialize the backfill report."""
        return {
            "scanned": self.scanned,
            "changed": self.changed,
            "missing_source": self.missing_source,
        }


def backfill_identity_rows(
    rows: Iterable[Any],
    *,
    apply: bool = False,
    index_version: str | None = None,
    indexed_at: datetime | None = None,
) -> IdentityBackfillReport:
    """Fill missing hash, version, status, and timestamp fields on ORM rows."""
    changed = 0
    missing_source = 0
    scanned = 0
    version = index_version or current_index_version()
    timestamp = indexed_at or datetime.now(KST)
    for row in rows:
        scanned += 1
        if not getattr(row, "source_table", None) or not getattr(row, "source_row_id", None):
            missing_source += 1
            continue
        row_changed = False
        if not getattr(row, "content_hash", None):
            row.content_hash = chunk_content_hash(getattr(row, "title", None), getattr(row, "content", ""))
            row_changed = True
        if not getattr(row, "index_version", None):
            row.index_version = version
            row_changed = True
        if not getattr(row, "index_status", None):
            row.index_status = ACTIVE_INDEX_STATUS
            row_changed = True
        if not getattr(row, "indexed_at", None):
            row.indexed_at = timestamp
            row_changed = True
        changed += int(row_changed)
        if not apply and row_changed:
            for field in ("content_hash", "index_version", "index_status", "indexed_at"):
                setattr(row, field, None)
    return IdentityBackfillReport(scanned, changed, missing_source)
