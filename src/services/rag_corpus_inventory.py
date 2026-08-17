"""Read-only inventory and incremental diff calculations for the RAG corpus."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from src.services.rag_index_identity import chunk_content_hash


@dataclass(frozen=True)
class SourceInventory:
    """Summarize one source iterator against the existing sparse index."""

    source: str
    documents_generated: int
    chunks_generated: int
    unchanged: int
    new: int
    updated: int
    deleted: int | None
    estimated_embedding_requests: int
    duplicate_identities: int
    invalid_identities: int
    missing_metadata: int
    error: str | None = None
    elapsed_ms: float = 0.0
    deleted_identities: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Serialize one source inventory row."""
        return {
            "source": self.source,
            "source_rows": self.documents_generated,
            "documents_generated": self.documents_generated,
            "chunks_generated": self.chunks_generated,
            "unchanged": self.unchanged,
            "new": self.new,
            "updated": self.updated,
            "deleted": self.deleted,
            "estimated_embedding_requests": self.estimated_embedding_requests,
            "duplicate_identities": self.duplicate_identities,
            "invalid_identities": self.invalid_identities,
            "missing_metadata": self.missing_metadata,
            "error": self.error,
            "elapsed_ms": self.elapsed_ms,
            "deleted_identities": list(self.deleted_identities),
        }


@dataclass(frozen=True)
class CorpusInventory:
    """Aggregate source inventories into an acceptance-friendly report."""

    sources: tuple[SourceInventory, ...]
    complete_scope: bool

    @property
    def has_defects(self) -> bool:
        """Return whether identity or metadata defects were found."""
        return any(
            source.error or source.duplicate_identities or source.invalid_identities or source.missing_metadata
            for source in self.sources
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize source rows and aggregate counts."""
        fields = (
            "documents_generated",
            "chunks_generated",
            "unchanged",
            "new",
            "updated",
            "estimated_embedding_requests",
            "duplicate_identities",
            "invalid_identities",
            "missing_metadata",
            "elapsed_ms",
        )
        totals = {field: sum(getattr(source, field) for source in self.sources) for field in fields}
        deleted_values = [source.deleted for source in self.sources]
        totals["deleted"] = sum(value for value in deleted_values if value is not None) if self.complete_scope else None
        return {
            "complete_scope": self.complete_scope,
            "source_count": len(self.sources),
            "total_sources": len(self.sources),
            "total_rows": totals["documents_generated"],
            "total_chunks": totals["chunks_generated"],
            "totals": totals,
            "has_defects": self.has_defects,
            "sources": [source.to_dict() for source in self.sources],
        }


def inventory_source_chunks(
    source: str,
    chunks: Iterable[Mapping[str, Any]],
    existing_rows: Iterable[Mapping[str, Any] | object],
    *,
    complete_scope: bool = True,
) -> SourceInventory:
    """Compare generated source chunks with canonical sparse rows."""
    started = perf_counter()
    existing = {_source_key(row): row for row in existing_rows if _source_key(row) is not None}
    generated_tables: set[str] = set()
    seen: set[str] = set()
    documents_generated = 0
    unchanged = 0
    new = 0
    updated = 0
    duplicate_identities = 0
    invalid_identities = 0
    missing_metadata = 0

    for chunk in chunks:
        documents_generated += 1
        source_table = _text(chunk.get("source_table"))
        if source_table:
            generated_tables.add(source_table)
        source_key = _source_key(chunk)
        if source_key is None:
            invalid_identities += 1
            continue
        if source_key in seen:
            duplicate_identities += 1
            continue
        seen.add(source_key)
        meta = chunk.get("meta") or {}
        if not chunk.get("document_type") and not meta.get("document_type"):
            missing_metadata += 1
        current = existing.get(source_key)
        if current is None:
            new += 1
            continue
        generated_hash = chunk_content_hash(chunk.get("title"), str(chunk.get("content") or ""))
        current_hash = _value(current, "content_hash")
        if current_hash == generated_hash:
            unchanged += 1
        else:
            updated += 1

    existing_for_source = {key for key in existing if _source_table_from_key(key) in generated_tables}
    deleted_identities = tuple(sorted(existing_for_source - seen)) if complete_scope else ()
    deleted = None if not complete_scope else len(deleted_identities)
    return SourceInventory(
        source=source,
        documents_generated=documents_generated,
        chunks_generated=documents_generated,
        unchanged=unchanged,
        new=new,
        updated=updated,
        deleted=deleted,
        estimated_embedding_requests=new + updated,
        duplicate_identities=duplicate_identities,
        invalid_identities=invalid_identities,
        missing_metadata=missing_metadata,
        elapsed_ms=round((perf_counter() - started) * 1000, 3),
        deleted_identities=deleted_identities,
    )


def failed_source_inventory(source: str, error: Exception) -> SourceInventory:
    """Build a source row for a read-only iterator failure."""
    return SourceInventory(
        source=source,
        documents_generated=0,
        chunks_generated=0,
        unchanged=0,
        new=0,
        updated=0,
        deleted=None,
        estimated_embedding_requests=0,
        duplicate_identities=0,
        invalid_identities=0,
        missing_metadata=0,
        error=f"{type(error).__name__}: {error}",
    )


def _source_key(row: Mapping[str, Any] | object) -> str | None:
    source_table = _value(row, "source_table")
    source_row_id = _value(row, "source_row_id")
    if source_table is None or source_row_id is None or not str(source_table) or not str(source_row_id):
        return None
    return f"{source_table}:{source_row_id}"


def _value(row: Mapping[str, Any] | object, key: str) -> object:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _text(value: object) -> str:
    """Normalize a source-table value for scope comparisons."""
    return str(value).strip() if value is not None else ""


def _source_table_from_key(source_key: str) -> str:
    """Extract the source table from a canonical chunk key."""
    return source_key.partition(":")[0]
