"""Point-in-time reconciliation of RAG identity manifests across independent stores."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator
    from pathlib import Path

UNEXPLAINED_ISSUES = (
    "CONTENT_HASH_MISMATCH",
    "CONTENT_HASH_MISSING",
    "INDEX_VERSION_MISMATCH",
    "INDEX_VERSION_MISSING",
    "EMBEDDING_MISSING",
    "INDEX_STATUS_MISMATCH",
    "MISSING_IN_RIGHT",
    "MISSING_IN_LEFT",
)


@dataclass(frozen=True)
class ManifestEntry:
    """Represent one chunk identity row exported from a RAG store."""

    source_table: str
    source_row_id: str
    content_hash: str | None = None
    index_version: str | None = None
    index_status: str | None = None
    embedding_present: bool | None = None
    updated_at: datetime | None = None

    @property
    def key(self) -> str:
        """Return the stable source identity key."""
        return f"{self.source_table}:{self.source_row_id}"

    def to_manifest_dict(self) -> dict[str, object]:
        """Serialize into an NDJSON-compatible mapping."""
        return {
            "source_table": self.source_table,
            "source_row_id": self.source_row_id,
            "content_hash": self.content_hash,
            "index_version": self.index_version,
            "index_status": self.index_status,
            "embedding_present": self.embedding_present,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


def _normalize_timestamp(value: datetime) -> datetime:
    """Attach KST timezone to naive timestamps so comparisons stay consistent."""
    if value.tzinfo is None:
        return value.replace(tzinfo=KST)
    return value


def parse_updated_at(raw: object) -> datetime | None:
    """Parse an ISO timestamp string, normalizing naive values to KST."""
    if raw is None:
        return None
    parsed = datetime.fromisoformat(str(raw))
    return _normalize_timestamp(parsed)


def _optional_str(row: dict[str, object], key: str) -> str | None:
    """Read an optional string field from a manifest row."""
    value = row.get(key)
    return str(value) if value else None


def entry_from_manifest_row(row: dict[str, object]) -> ManifestEntry:
    """Build a ManifestEntry from one parsed NDJSON line."""
    embedding = row.get("embedding_present")
    return ManifestEntry(
        source_table=str(row["source_table"]),
        source_row_id=str(row["source_row_id"]),
        content_hash=_optional_str(row, "content_hash"),
        index_version=_optional_str(row, "index_version"),
        index_status=_optional_str(row, "index_status"),
        embedding_present=None if embedding is None else bool(embedding),
        updated_at=parse_updated_at(row.get("updated_at")),
    )


def read_manifest(path: Path) -> Iterator[ManifestEntry]:
    """Yield entries from an NDJSON identity manifest."""
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield entry_from_manifest_row(json.loads(stripped))


def write_manifest(entries: Iterable[ManifestEntry], path: Path) -> int:
    """Write entries as NDJSON and return the written row count."""
    written = 0
    with path.open("w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry.to_manifest_dict(), ensure_ascii=False) + "\n")
            written += 1
    return written


@dataclass(frozen=True)
class ReconciliationReport:
    """Summarize point-in-time differences between two identity manifests."""

    left_label: str
    right_label: str
    left_count: int
    right_count: int
    as_of: datetime | None
    unexplained_issues: dict[str, tuple[str, ...]] = field(default_factory=dict)
    healthy_keys: tuple[str, ...] = ()
    time_explainable_keys: tuple[str, ...] = ()
    left_only_after_cutoff: tuple[str, ...] = ()
    right_only_after_cutoff: tuple[str, ...] = ()

    @property
    def _key_union(self) -> set[str]:
        """Collect every identity key covered by this report."""
        union: set[str] = set(self.healthy_keys)
        union.update(self.time_explainable_keys)
        union.update(self.left_only_after_cutoff)
        union.update(self.right_only_after_cutoff)
        for keys in self.unexplained_issues.values():
            union.update(keys)
        return union

    @property
    def total_union_count(self) -> int:
        """Count distinct keys across both manifests."""
        return len(self._key_union)

    @property
    def common_count(self) -> int:
        """Count keys present on both sides regardless of explanation."""
        singles = set(self.unexplained_issues.get("MISSING_IN_RIGHT", ()))
        singles.update(self.unexplained_issues.get("MISSING_IN_LEFT", ()))
        return self.total_union_count - len(singles)

    @property
    def unexplained_count(self) -> int:
        """Count distinct keys with at least one unexplained issue."""
        union: set[str] = set()
        for keys in self.unexplained_issues.values():
            union.update(keys)
        return len(union)

    @property
    def is_clean(self) -> bool:
        """Return whether no unexplained drift remains under the as-of policy."""
        return self.unexplained_count == 0

    def _count_by_source(self, keys: Iterable[str], entries: dict[str, ManifestEntry]) -> dict[str, int]:
        """Group keys by their source table using whichever side holds them."""
        counts: dict[str, int] = {}
        for key in keys:
            entry = entries.get(key)
            table = entry.source_table if entry else key.split(":", 1)[0]
            counts[table] = counts.get(table, 0) + 1
        return dict(sorted(counts.items()))

    def to_summary_dict(self, left: dict[str, ManifestEntry], right: dict[str, ManifestEntry]) -> dict[str, object]:
        """Render the full comparison payload including per-source breakdowns."""
        left_only = list(self.unexplained_issues.get("MISSING_IN_RIGHT", ()))
        right_only = list(self.unexplained_issues.get("MISSING_IN_LEFT", ()))
        hash_mismatch = list(self.unexplained_issues.get("CONTENT_HASH_MISMATCH", []))
        return {
            "report_type": "rag_identity_manifest_reconciliation",
            "left": self.left_label,
            "right": self.right_label,
            "left_count": self.left_count,
            "right_count": self.right_count,
            "common_count": self.common_count,
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "unexplained_count": self.unexplained_count,
            "unexplained_by_issue": {issue: len(keys) for issue, keys in self.unexplained_issues.items()},
            "unexplained_left_only_by_source": self._count_by_source(left_only, left),
            "unexplained_right_only_by_source": self._count_by_source(right_only, right),
            "unexplained_hash_mismatch_by_source": self._count_by_source(hash_mismatch, {**left, **right}),
            "time_explainable_count": len(self.time_explainable_keys),
            "left_only_after_cutoff_count": len(self.left_only_after_cutoff),
            "right_only_after_cutoff_count": len(self.right_only_after_cutoff),
            "is_clean": self.is_clean,
        }


def _pair_findings(left: ManifestEntry, right: ManifestEntry) -> list[tuple[str, str]]:
    """Compare co-present entries and return (issue, key) pairs."""
    findings: list[tuple[str, str]] = []
    key = left.key
    if not left.content_hash or not right.content_hash:
        findings.append(("CONTENT_HASH_MISSING", key))
    elif left.content_hash != right.content_hash:
        findings.append(("CONTENT_HASH_MISMATCH", key))
    if not left.index_version or not right.index_version:
        findings.append(("INDEX_VERSION_MISSING", key))
    elif left.index_version != right.index_version:
        findings.append(("INDEX_VERSION_MISMATCH", key))
    if left.index_status != right.index_status:
        findings.append(("INDEX_STATUS_MISMATCH", key))
    if left.embedding_present is False or right.embedding_present is False:
        findings.append(("EMBEDDING_MISSING", key))
    return findings


def _changed_after_cutoff(entry: ManifestEntry | None, as_of: datetime) -> bool:
    """Return whether the entry changed outside the shared comparison window."""
    return entry is not None and entry.updated_at is not None and entry.updated_at > as_of


def reconcile_manifests(
    left_entries: Iterable[ManifestEntry],
    right_entries: Iterable[ManifestEntry],
    *,
    left_label: str = "left",
    right_label: str = "right",
    as_of: datetime | None = None,
) -> ReconciliationReport:
    """Classify every identity-key difference as unexplained or time-explainable."""
    left_map = {entry.key: entry for entry in left_entries}
    right_map = {entry.key: entry for entry in right_entries}
    issues: dict[str, list[str]] = {}
    healthy: list[str] = []
    time_explainable: list[str] = []
    left_only_after: list[str] = []
    right_only_after: list[str] = []

    for key in sorted(left_map.keys() | right_map.keys()):
        left_entry = left_map.get(key)
        right_entry = right_map.get(key)
        if left_entry is not None and right_entry is not None:
            if as_of is not None and (
                _changed_after_cutoff(left_entry, as_of) or _changed_after_cutoff(right_entry, as_of)
            ):
                time_explainable.append(key)
                continue
            findings = _pair_findings(left_entry, right_entry)
            if findings:
                for issue, finding_key in findings:
                    issues.setdefault(issue, []).append(finding_key)
            else:
                healthy.append(key)
            continue
        if left_entry is not None:
            holder, missing_issue = left_entry, "MISSING_IN_RIGHT"
        else:
            holder, missing_issue = right_entry, "MISSING_IN_LEFT"
        if as_of is not None and _changed_after_cutoff(holder, as_of):
            if missing_issue == "MISSING_IN_RIGHT":
                left_only_after.append(key)
            else:
                right_only_after.append(key)
            continue
        issues.setdefault(missing_issue, []).append(key)

    return ReconciliationReport(
        left_label=left_label,
        right_label=right_label,
        left_count=len(left_map),
        right_count=len(right_map),
        as_of=as_of,
        unexplained_issues={issue: tuple(keys) for issue, keys in sorted(issues.items())},
        healthy_keys=tuple(healthy),
        time_explainable_keys=tuple(time_explainable),
        left_only_after_cutoff=tuple(left_only_after),
        right_only_after_cutoff=tuple(right_only_after),
    )
