"""Apply an identity rekey manifest to the vector store.

The rekey tools update the sparse store and say nothing about the vector store,
because until the vectors moved to pgvector there was nothing else to update.
Now a chunk carries the same identity in two places, so a rekey applied to one
of them leaves the other holding the old key: the hybrid retriever then sees one
chunk as two documents, and content the sparse side tombstoned stays retrievable
by the dense side.

The dispositions mirror ``apply_rag_rekey`` exactly, because mirroring is the
whole point -- a store that disagrees with its pair is what this exists to
prevent:

* ``SAFE_REKEY`` moves ``source_row_id`` to the natural key.
* ``TARGET_EXISTS_SAME_CONTENT`` tombstones the legacy duplicate: the natural
  target already holds the same content, so the row is redundant rather than
  lost.
* Everything else (orphans, content mismatches, collisions) is skipped, because
  the sparse side skips it too and a decision the sparse side has not made is
  not one this tool should make alone.

No embedding is touched. The vector does not depend on the key it is filed
under, so the copy stays valid and nothing is re-embedded.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from pathlib import Path

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DISPOSITION_REKEY = "SAFE_REKEY"
DISPOSITION_TOMBSTONE = "TARGET_EXISTS_SAME_CONTENT"
SUPPORTED_DISPOSITIONS = (DISPOSITION_REKEY, DISPOSITION_TOMBSTONE)

#: Cap on retained sample keys per outcome; the counts carry the rest.
SAMPLE_CAP = 5

_REKEY_SQL = text(
    "UPDATE rag_chunks SET source_row_id = :natural_id, updated_at = :now "
    "WHERE source_table = :source_table AND source_row_id = :legacy_id AND content_hash = :content_hash"
)
_TOMBSTONE_SQL = text(
    "UPDATE rag_chunks SET index_status = 'DELETED', updated_at = :now "
    "WHERE source_table = :source_table AND source_row_id = :legacy_id "
    "AND content_hash = :content_hash AND index_status <> 'DELETED'"
)


@dataclass(frozen=True)
class RekeyEntry:
    """One manifest entry, in the shape the vector store needs."""

    chunk_id: int
    disposition: str
    source_table: str
    legacy_source_row_id: str
    natural_source_row_id: str | None
    content_hash: str | None


@dataclass
class VectorRekeyReport:
    """Count what an apply run did, per disposition."""

    rekeyed: int = 0
    tombstoned: int = 0
    planned_rekeyed: int = 0
    planned_tombstoned: int = 0
    already_applied: int = 0
    skipped_unsupported: int = 0
    missing: int = 0
    conflicted: int = 0
    failed: int = 0
    samples: dict[str, list[str]] = field(default_factory=dict)

    @property
    def summary(self) -> str:
        """Return a one-line rendering for logs and CLI output."""
        return (
            f"rekeyed={self.rekeyed} tombstoned={self.tombstoned} already={self.already_applied} "
            f"skipped={self.skipped_unsupported} missing={self.missing} conflicted={self.conflicted} "
            f"failed={self.failed}"
        )

    def note(self, kind: str, key: str) -> None:
        """Record a sample key for a counted outcome, capped per kind."""
        bucket = self.samples.setdefault(kind, [])
        if len(bucket) < SAMPLE_CAP:
            bucket.append(key)


def load_entries(path: Path) -> list[RekeyEntry]:
    """Read the census manifest the rekey tools share."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("entries")
    if not isinstance(entries, list):
        message = f"manifest has no entries list: {path}"
        raise TypeError(message)
    loaded: list[RekeyEntry] = []
    for raw in entries:
        source_table = str(raw.get("source_table") or "")
        legacy_id = raw.get("legacy_source_row_id")
        if not source_table or legacy_id is None:
            continue
        loaded.append(
            RekeyEntry(
                chunk_id=int(raw.get("chunk_id") or 0),
                disposition=str(raw.get("disposition") or ""),
                source_table=source_table,
                legacy_source_row_id=str(legacy_id),
                natural_source_row_id=(
                    str(raw["natural_source_row_id"]) if raw.get("natural_source_row_id") is not None else None
                ),
                content_hash=raw.get("legacy_content_hash"),
            )
        )
    return loaded


def _params(entry: RekeyEntry, now: datetime) -> dict[str, object]:
    """Return the bind parameters shared by both statements."""
    return {
        "source_table": entry.source_table,
        "legacy_id": entry.legacy_source_row_id,
        "content_hash": entry.content_hash,
        "now": now,
    }


def _load_store_state(session: Session) -> dict[str, tuple[str | None, str]]:
    """Read the identities the store holds now, keyed by ``table:row_id``.

    One query instead of one per entry. The per-entry version spent twenty-five
    minutes crossing the network and then died when the connection did, which is
    both the slowest and the most fragile way to move 120k keys.
    """
    rows = session.execute(text("SELECT source_table, source_row_id, content_hash, index_status FROM rag_chunks")).all()
    return {f"{row[0]}:{row[1]}": (row[2], str(row[3] or "")) for row in rows}


def _classify(entry: RekeyEntry, state: dict[str, tuple[str | None, str]], report: VectorRekeyReport) -> str:
    """Decide what this entry needs against the state already read.

    Returns ``"rekey"`` or ``"tombstone"`` to plan an update, or ``""`` when the
    entry is counted (unsupported, already applied, missing, or a conflict) and
    nothing should be written.
    """
    key = f"{entry.source_table}:{entry.legacy_source_row_id}"
    if entry.disposition not in SUPPORTED_DISPOSITIONS or (
        entry.disposition == DISPOSITION_REKEY and not entry.natural_source_row_id
    ):
        report.skipped_unsupported += 1
        report.note("skipped_unsupported", key)
        return ""
    if entry.disposition == DISPOSITION_REKEY:
        return _classify_rekey(entry, state, report, key)
    return _classify_tombstone(entry, state, report, key)


def _classify_rekey(
    entry: RekeyEntry,
    state: dict[str, tuple[str | None, str]],
    report: VectorRekeyReport,
    key: str,
) -> str:
    """Decide one rekey entry against the store's current identities."""
    current = state.get(key)
    natural_present = f"{entry.source_table}:{entry.natural_source_row_id}" in state
    if current is None and natural_present:
        # The legacy row is gone and the natural one is present: this entry was
        # already applied, most likely by an interrupted earlier run.
        report.already_applied += 1
        return ""
    if current is not None and natural_present:
        # The census said this target did not exist; it does now. Moving the
        # legacy row onto it would violate the identity index, so the row is
        # left for a human rather than merged on a guess.
        report.conflicted += 1
        report.note("conflicted", key)
        return ""
    if current is None or current[0] != entry.content_hash:
        report.missing += 1
        report.note("missing", key)
        return ""
    return "rekey"


def _classify_tombstone(
    entry: RekeyEntry,
    state: dict[str, tuple[str | None, str]],
    report: VectorRekeyReport,
    key: str,
) -> str:
    """Decide one tombstone entry against the store's current identities."""
    current = state.get(key)
    if current is None or current[0] != entry.content_hash:
        report.missing += 1
        report.note("missing", key)
        return ""
    if current[1] == "DELETED":
        report.already_applied += 1
        return ""
    return "tombstone"


def _execute_chunks(  # noqa: PLR0913 - the statement, its params, and the counter they feed are one unit
    session: Session,
    statement: object,
    params: list[dict[str, object]],
    report: VectorRekeyReport,
    *,
    attribute: str,
    chunk_size: int,
) -> None:
    """Apply the planned updates in chunks, committing each one."""
    for start in range(0, len(params), chunk_size):
        chunk = params[start : start + chunk_size]
        try:
            affected = session.execute(statement, chunk).rowcount or 0
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            report.failed += len(chunk)
            for item in chunk[:SAMPLE_CAP]:
                report.note("failed", f"{item['source_table']}:{item['legacy_id']}")
            logger.exception("Vector rekey chunk failed (%d rows)", len(chunk))
            continue
        setattr(report, attribute, getattr(report, attribute) + affected)


def apply_entries(
    session: Session,
    entries: Iterable[RekeyEntry],
    *,
    dry_run: bool = True,
    now: datetime | None = None,
    chunk_size: int = 500,
) -> VectorRekeyReport:
    """Move or tombstone the identities this manifest names, and count the rest.

    The store's state is read once and the updates are sent in chunks, so the run
    is bounded by a handful of round trips rather than one per entry. A row that
    cannot be matched is counted rather than ignored: it means the two stores
    have already diverged before this ran, which is the state the report should
    make visible instead of hiding behind a success count.
    """
    stamp = now or datetime.now(KST)
    report = VectorRekeyReport()
    state = _load_store_state(session)
    rekey_params: list[dict[str, object]] = []
    tombstone_params: list[dict[str, object]] = []
    for entry in entries:
        action = _classify(entry, state, report)
        if not action:
            continue
        params = _params(entry, stamp)
        if action == "rekey":
            params["natural_id"] = entry.natural_source_row_id
            rekey_params.append(params)
            report.planned_rekeyed += 1
        else:
            tombstone_params.append(params)
            report.planned_tombstoned += 1
    if dry_run:
        return report
    report.planned_rekeyed = 0
    report.planned_tombstoned = 0
    _execute_chunks(session, _REKEY_SQL, rekey_params, report, attribute="rekeyed", chunk_size=chunk_size)
    _execute_chunks(session, _TOMBSTONE_SQL, tombstone_params, report, attribute="tombstoned", chunk_size=chunk_size)
    return report


def summarize_manifest(entries: Sequence[RekeyEntry]) -> dict[str, int]:
    """Count manifest entries by disposition for a preview."""
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.disposition] = counts.get(entry.disposition, 0) + 1
    return counts
