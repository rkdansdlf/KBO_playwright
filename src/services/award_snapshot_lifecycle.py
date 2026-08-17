"""Lifecycle helpers for superseding obsolete award snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SupersedeCandidate:
    """Identify a pending snapshot replaced by a later successful snapshot."""

    source_key: str
    snapshot_id: int
    replacement_snapshot_id: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize a supersede candidate."""
        return {
            "source_key": self.source_key,
            "snapshot_id": self.snapshot_id,
            "replacement_snapshot_id": self.replacement_snapshot_id,
        }


def find_supersede_candidates(
    source_rows: list[object],
    snapshot_rows: list[object],
) -> tuple[SupersedeCandidate, ...]:
    """Find pending award snapshots followed by a later done snapshot."""
    source_keys = {_value(row, "id"): _value(row, "source_key") for row in source_rows}
    by_source: dict[object, list[object]] = {}
    for snapshot in snapshot_rows:
        by_source.setdefault(_value(snapshot, "data_source_id"), []).append(snapshot)

    candidates: list[SupersedeCandidate] = []
    for data_source_id, snapshots in by_source.items():
        done = [row for row in snapshots if _value(row, "parse_status") == "done"]
        for pending in snapshots:
            if _value(pending, "parse_status") != "pending":
                continue
            later = [
                row
                for row in done
                if _value(row, "fetched_at") is not None
                and _value(pending, "fetched_at") is not None
                and _value(row, "fetched_at") > _value(pending, "fetched_at")
            ]
            if not later:
                continue
            replacement = min(later, key=lambda row: _value(row, "fetched_at"))
            candidates.append(
                SupersedeCandidate(
                    source_key=str(source_keys.get(data_source_id) or data_source_id),
                    snapshot_id=int(_value(pending, "id")),
                    replacement_snapshot_id=int(_value(replacement, "id")),
                ),
            )
    return tuple(sorted(candidates, key=lambda candidate: (candidate.source_key, candidate.snapshot_id)))


def _value(row: object, key: str) -> object:
    """Read a field from an ORM row or mapping-like test object."""
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)
