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
    failed: int = 0
    samples: dict[str, list[str]] = field(default_factory=dict)

    @property
    def summary(self) -> str:
        """Return a one-line rendering for logs and CLI output."""
        return (
            f"rekeyed={self.rekeyed} tombstoned={self.tombstoned} already={self.already_applied} "
            f"skipped={self.skipped_unsupported} missing={self.missing} failed={self.failed}"
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


def apply_entries(
    session: Session,
    entries: Iterable[RekeyEntry],
    *,
    dry_run: bool = True,
    now: datetime | None = None,
) -> VectorRekeyReport:
    """Move or tombstone the identities this manifest names, and count the rest.

    A row that cannot be matched is counted rather than ignored: it means the two
    stores have already diverged before this ran, which is the state the report
    should make visible instead of hiding behind a success count.
    """
    stamp = now or datetime.now(KST)
    report = VectorRekeyReport()
    for entry in entries:
        _apply_one(session, entry, stamp, report, dry_run=dry_run)
    if not dry_run:
        session.commit()
    return report


def _statement_for(
    entry: RekeyEntry,
    stamp: datetime,
    report: VectorRekeyReport,
) -> tuple[object | None, str, dict[str, object]]:
    """Return the statement an entry needs, or mark it unsupported and return none."""
    key = f"{entry.source_table}:{entry.legacy_source_row_id}"
    if entry.disposition not in SUPPORTED_DISPOSITIONS:
        report.skipped_unsupported += 1
        report.note("skipped_unsupported", key)
        return None, "", {}
    if entry.disposition == DISPOSITION_REKEY and not entry.natural_source_row_id:
        report.skipped_unsupported += 1
        report.note("skipped_unsupported", key)
        return None, "", {}
    params = _params(entry, stamp)
    if entry.disposition == DISPOSITION_REKEY:
        params["natural_id"] = entry.natural_source_row_id
        return _REKEY_SQL, "rekeyed", params
    return _TOMBSTONE_SQL, "tombstoned", params


def _apply_one(
    session: Session,
    entry: RekeyEntry,
    stamp: datetime,
    report: VectorRekeyReport,
    *,
    dry_run: bool,
) -> None:
    """Apply one entry, counting the outcome without aborting the run."""
    statement, kind, params = _statement_for(entry, stamp, report)
    if statement is None:
        return
    key = f"{entry.source_table}:{entry.legacy_source_row_id}"
    if dry_run:
        if kind == "rekeyed":
            report.planned_rekeyed += 1
        else:
            report.planned_tombstoned += 1
        report.note(f"planned_{kind}", key)
        return
    try:
        affected = session.execute(statement, params).rowcount or 0
    except SQLAlchemyError:
        session.rollback()
        report.failed += 1
        report.note("failed", key)
        logger.exception("Vector rekey failed for %s", key)
        return
    if affected:
        if kind == "rekeyed":
            report.rekeyed += affected
        else:
            report.tombstoned += affected
    elif _already_applied(session, entry, kind):
        report.already_applied += 1
    else:
        report.missing += 1
        report.note("missing", key)


def _already_applied(session: Session, entry: RekeyEntry, kind: str) -> bool:
    """Return whether this entry has nothing left to do on the vector side."""
    if kind == "tombstoned":
        query = text(
            "SELECT COUNT(*) FROM rag_chunks WHERE source_table = :source_table "
            "AND source_row_id = :legacy_id AND index_status = 'DELETED'"
        )
    else:
        query = text(
            "SELECT COUNT(*) FROM rag_chunks WHERE source_table = :source_table AND source_row_id = :legacy_id"
        )
        if entry.natural_source_row_id:
            query = text(
                "SELECT COUNT(*) FROM rag_chunks WHERE source_table = :source_table AND source_row_id = :natural_id"
            )
            return bool(
                session.execute(
                    query, {"source_table": entry.source_table, "natural_id": entry.natural_source_row_id}
                ).scalar()
            )
    return bool(session.execute(query, _params(entry, datetime.now(KST))).scalar())


def summarize_manifest(entries: Sequence[RekeyEntry]) -> dict[str, int]:
    """Count manifest entries by disposition for a preview."""
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.disposition] = counts.get(entry.disposition, 0) + 1
    return counts
