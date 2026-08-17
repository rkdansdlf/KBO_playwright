"""Compare canonical sparse rows with pgvector rows by shared chunk identity."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class IndexConsistencyFinding:
    """Describe one mismatch between the sparse and vector indexes."""

    source_key: str
    issue: str
    primary_hash: str | None = None
    vector_hash: str | None = None
    primary_version: str | None = None
    vector_version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize a consistency finding."""
        return {
            "source_key": self.source_key,
            "issue": self.issue,
            "primary_hash": self.primary_hash,
            "vector_hash": self.vector_hash,
            "primary_version": self.primary_version,
            "vector_version": self.vector_version,
        }


@dataclass(frozen=True)
class IndexConsistencyReport:
    """Summarize identity and content mismatches across both indexes."""

    primary_count: int
    vector_count: int
    findings: tuple[IndexConsistencyFinding, ...]
    total_keys: int = 0
    stale_keys: tuple[str, ...] = ()
    deleted_keys: tuple[str, ...] = ()

    @property
    def is_consistent(self) -> bool:
        """Return whether no cross-index mismatch was found."""
        return not self.findings and not self.stale_keys

    def to_dict(self) -> dict[str, Any]:
        """Serialize the consistency report."""
        finding_keys = {finding.source_key for finding in self.findings}
        issue_keys = {
            issue: {finding.source_key for finding in self.findings if finding.issue == issue}
            for issue in (
                "MISSING_IN_VECTOR",
                "MISSING_IN_PRIMARY",
                "CONTENT_HASH_MISMATCH",
                "CONTENT_HASH_MISSING",
                "INDEX_VERSION_MISMATCH",
                "INDEX_VERSION_MISSING",
                "VECTOR_EMBEDDING_MISSING",
            )
        }
        sparse_only = len(issue_keys["MISSING_IN_VECTOR"])
        vector_only = len(issue_keys["MISSING_IN_PRIMARY"])
        hash_mismatch = len(issue_keys["CONTENT_HASH_MISMATCH"] | issue_keys["CONTENT_HASH_MISSING"])
        version_mismatch = len(issue_keys["INDEX_VERSION_MISMATCH"] | issue_keys["INDEX_VERSION_MISSING"])
        embedding_missing = len(issue_keys["VECTOR_EMBEDDING_MISSING"])
        total = self.total_keys or max(self.primary_count, self.vector_count)
        non_healthy = finding_keys | set(self.stale_keys) | set(self.deleted_keys)
        return {
            "primary_count": self.primary_count,
            "vector_count": self.vector_count,
            "total": total,
            "healthy": max(total - len(non_healthy), 0),
            "sparse_only": sparse_only,
            "vector_only": vector_only,
            "orphan": vector_only,
            "hash_mismatch": hash_mismatch,
            "version_mismatch": version_mismatch,
            "embedding_missing": embedding_missing,
            "stale": len(self.stale_keys),
            "deleted": len(self.deleted_keys),
            "finding_count": len(self.findings),
            "consistent": self.is_consistent,
            "findings": [finding.to_dict() for finding in self.findings],
        }


def _value(row: Mapping[str, Any] | object, key: str) -> object:
    """Read a field from a mapping or ORM row."""
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key, None)


def _has_field(row: Mapping[str, Any] | object, key: str) -> bool:
    """Return whether a row shape exposes a field, including nullable ORM columns."""
    if isinstance(row, Mapping):
        return key in row
    return hasattr(row, key)


def _source_key(row: Mapping[str, Any] | object) -> str:
    """Build the stable source identity for a row."""
    return f"{_value(row, 'source_table')}:{_value(row, 'source_row_id')}"


def _embedding_is_missing(row: Mapping[str, Any] | object) -> bool:
    """Return whether a vector projection lacks its embedding."""
    if _has_field(row, "embedding_present"):
        return not bool(_value(row, "embedding_present"))
    return _has_field(row, "embedding") and _value(row, "embedding") is None


def compare_index_rows(
    primary_rows: Iterable[Mapping[str, Any] | object],
    vector_rows: Iterable[Mapping[str, Any] | object],
) -> IndexConsistencyReport:
    """Compare row identity, content hash, version, and lifecycle status."""
    primary = {_source_key(row): row for row in primary_rows}
    vector = {_source_key(row): row for row in vector_rows}
    findings: list[IndexConsistencyFinding] = []
    stale_keys: set[str] = set()
    deleted_keys: set[str] = set()

    for source_key in sorted(primary.keys() | vector.keys()):
        primary_row = primary.get(source_key)
        vector_row = vector.get(source_key)
        primary_status = _value(primary_row, "index_status") if primary_row is not None else None
        vector_status = _value(vector_row, "index_status") if vector_row is not None else None
        if primary_status in {"STALE", "REINDEX_REQUIRED", "DELETE_PENDING"} or vector_status in {
            "STALE",
            "REINDEX_REQUIRED",
            "DELETE_PENDING",
        }:
            stale_keys.add(source_key)
        if primary_status in {"DELETED", "TOMBSTONED"} or vector_status in {"DELETED", "TOMBSTONED"}:
            deleted_keys.add(source_key)
        if primary_row is None:
            findings.append(IndexConsistencyFinding(source_key, "MISSING_IN_PRIMARY"))
            continue
        if vector_row is None:
            findings.append(IndexConsistencyFinding(source_key, "MISSING_IN_VECTOR"))
            continue

        primary_hash = _value(primary_row, "content_hash")
        vector_hash = _value(vector_row, "content_hash")
        primary_version = _value(primary_row, "index_version")
        vector_version = _value(vector_row, "index_version")
        finding_kwargs = {
            "primary_hash": primary_hash,
            "vector_hash": vector_hash,
            "primary_version": primary_version,
            "vector_version": vector_version,
        }
        if not primary_hash or not vector_hash or primary_hash != vector_hash:
            issue = "CONTENT_HASH_MISMATCH" if primary_hash and vector_hash else "CONTENT_HASH_MISSING"
            findings.append(IndexConsistencyFinding(source_key, issue, **finding_kwargs))
        if not primary_version or not vector_version or primary_version != vector_version:
            issue = "INDEX_VERSION_MISMATCH" if primary_version and vector_version else "INDEX_VERSION_MISSING"
            findings.append(IndexConsistencyFinding(source_key, issue, **finding_kwargs))
        if _embedding_is_missing(vector_row):
            findings.append(IndexConsistencyFinding(source_key, "VECTOR_EMBEDDING_MISSING", **finding_kwargs))
        primary_status = primary_status or "ACTIVE"
        vector_status = vector_status or "ACTIVE"
        if primary_status != vector_status:
            findings.append(IndexConsistencyFinding(source_key, "INDEX_STATUS_MISMATCH", **finding_kwargs))

    return IndexConsistencyReport(
        len(primary),
        len(vector),
        tuple(findings),
        total_keys=len(primary.keys() | vector.keys()),
        stale_keys=tuple(sorted(stale_keys)),
        deleted_keys=tuple(sorted(deleted_keys)),
    )


def audit_index_sessions(primary_session: Session, vector_session: Session) -> IndexConsistencyReport:
    """Load lightweight identity projections from both indexes and compare them."""
    from sqlalchemy import select

    from src.models.rag_chunk import RagChunk
    from src.models.rag_chunk_vector import RagChunkVector

    identity_columns = (
        "source_table",
        "source_row_id",
        "content_hash",
        "index_version",
        "index_status",
    )
    primary_rows = (
        primary_session.execute(select(*(getattr(RagChunk, column) for column in identity_columns))).mappings().all()
    )
    vector_rows = (
        vector_session.execute(
            select(
                *(getattr(RagChunkVector, column) for column in identity_columns),
                RagChunkVector.embedding.is_not(None).label("embedding_present"),
            )
        )
        .mappings()
        .all()
    )
    return compare_index_rows(primary_rows, vector_rows)
