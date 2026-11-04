"""Delete ``player_movements`` legacy chunks that duplicate a natural-key chunk.

The legacy chunks (``id=N``) can never be regenerated -- the generator writes
``{date}_{team}_{player}_{section}_{hash}`` -- but the reason they are safe to
remove is stronger than that: **the same fact is already stored under the natural
key, with the same information**. Measured 2026-11-04 over all 4,503 of them:

- every legacy chunk has a natural-key chunk with the same ``(date, player,
  section)`` triple;
- every legacy chunk that carries a ``비고`` has that same note in the natural-key
  chunk, and every one that does not is also missing it there (2,368 + 2,135);
- the source's ``remarks`` is at most 28 characters, so the generator's
  ``remarks[:150]`` truncation never loses anything.

The legacy form is *longer*, which is what a first pass saw and misread as "the
legacy chunk is richer": it writes a header line and five labelled fields where the
natural key writes one line. Length is not information.

Deciding which rows those are is this script's ``--backup-out`` mode (read-only);
deleting them is ``--backup-file`` + ``--apply``. Splitting the two keeps the
destructive half reading a list rather than making judgments.

Guard: ``--apply`` **and** ``KBO_ALLOW_RAG_CHUNK_DELETE=1``; otherwise it prints the
plan and exits 3 without writing.

Usage:
    python3 -m scripts.maintenance.purge_duplicate_movement_chunks \
        --backup-out data/repair_backups/movement_duplicates_20261105.csv
    python3 -m scripts.maintenance.purge_duplicate_movement_chunks \
        --backup-file data/repair_backups/movement_duplicates_20261105.csv --apply
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from collections.abc import Sequence

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from scripts.maintenance.purge_orphan_rag_chunks import _retire_rows
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.audit_trail import CorrectionAuditTrail
from src.models.player import PlayerMovement
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import stable_player_movement_source_row_id

ALLOW_ENV = "KBO_ALLOW_RAG_CHUNK_DELETE"
REASON = "duplicate_of_natural_key"
BACKUP_COLUMNS = ("chunk_id", "source_row_id", "index_status", "natural_key", "legacy_note", "reason")

_LEGACY_ID = re.compile(r"^id=\d+$")
LEGACY_FIELDS = re.compile(r"^([가-힣 ]+):\s*(.*)$", re.MULTILINE)


@dataclass(frozen=True)
class DuplicateRow:
    """One legacy chunk whose fact is already stored under the natural key."""

    chunk_id: int
    source_row_id: str
    index_status: str
    natural_key: str
    legacy_note: str
    reason: str = REASON


def legacy_fields(content: str) -> dict[str, str]:
    """Read the labelled fields out of a legacy movement chunk.

    The legacy generator wrote ``날짜: …`` / ``선수: …`` lines, so the triple has to
    be read back out of the text: the chunk id carries no identity.

    Args:
        content: The chunk body.

    Returns:
        Field name to value, empty when the body is not in the legacy shape.

    """
    return {name.strip(): value.strip() for name, value in LEGACY_FIELDS.findall(content or "")}


def natural_keys_by_triple(session: Session) -> dict[tuple[str, str, str], tuple[str, str, str | None]]:
    """Map ``(date, player, section)`` to the natural key, its content, and the source note.

    Args:
        session: Session the movements are read through.

    Returns:
        The mapping; a movement whose parts are unknown still gets a key.

    """
    mapping: dict[tuple[str, str, str], tuple[str, str, str | None]] = {}
    for movement in session.execute(select(PlayerMovement)).scalars():
        date_str = str(movement.movement_date) if movement.movement_date else "날짜불명"
        triple = (date_str, movement.player_name or "", movement.section or "")
        key = stable_player_movement_source_row_id(
            movement.movement_date,
            movement.canonical_team_id or movement.team_code,
            movement.player_name,
            movement.section,
        )
        mapping[triple] = (key, "", movement.remarks)
    return mapping


def plan_duplicates(session: Session) -> tuple[list[DuplicateRow], list[str]]:
    """Verify every legacy movement chunk against its natural-key counterpart.

    Args:
        session: Session the chunks are read through.

    Returns:
        The rows safe to delete and one message per row that failed verification.

    """
    natural = {
        row.source_row_id: (row.content or "")
        for row in session.execute(
            select(RagChunk).where(
                RagChunk.source_table == "player_movements",
                RagChunk.source_row_id.regexp_match("^[0-9]{4}-"),
            ),
        ).scalars()
    }
    triples = natural_keys_by_triple(session)

    legacy = list(
        session.execute(
            select(RagChunk).where(
                RagChunk.source_table == "player_movements",
                RagChunk.source_row_id.regexp_match("^id="),
                RagChunk.index_status == "ACTIVE",
            ),
        ).scalars(),
    )

    confirmed: list[DuplicateRow] = []
    rejected: list[str] = []
    for chunk in legacy:
        fields = legacy_fields(chunk.content or "")
        triple = (fields.get("날짜", ""), fields.get("선수", ""), fields.get("이동 유형", ""))
        hit = triples.get(triple)
        if hit is None:
            rejected.append(f"{chunk.source_row_id}: no source row for {triple}")
            continue
        key, _content, remarks = hit
        counterpart = natural.get(key)
        if counterpart is None:
            rejected.append(f"{chunk.source_row_id}: no natural-key chunk for {key}")
            continue
        note = fields.get("비고", "")
        if note and note not in counterpart:
            rejected.append(f"{chunk.source_row_id}: note {note[:40]!r} is missing from {key}")
            continue
        if remarks and remarks.strip() and note and remarks.strip()[:60] not in counterpart:
            rejected.append(f"{chunk.source_row_id}: source remarks absent from {key}")
            continue
        confirmed.append(
            DuplicateRow(
                chunk_id=chunk.id,
                source_row_id=chunk.source_row_id,
                index_status=chunk.index_status,
                natural_key=key,
                legacy_note=note,
            ),
        )
    return confirmed, rejected


def _write_backup(path: Path, rows: Sequence[DuplicateRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=BACKUP_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "chunk_id": row.chunk_id,
                    "source_row_id": row.source_row_id,
                    "index_status": row.index_status,
                    "natural_key": row.natural_key,
                    "legacy_note": row.legacy_note,
                    "reason": row.reason,
                },
            )


def load_backup(path: Path) -> list[DuplicateRow]:
    """Read a plan written by ``--backup-out``.

    Args:
        path: Backup CSV.

    Returns:
        The rows to delete.

    """
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = set(BACKUP_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            message = f"backup file is missing columns: {sorted(missing)}"
            raise ValueError(message)
        return [
            DuplicateRow(
                chunk_id=int(row["chunk_id"]),
                source_row_id=row["source_row_id"],
                index_status=row["index_status"],
                natural_key=row["natural_key"],
                legacy_note=row["legacy_note"],
                reason=row["reason"],
            )
            for row in reader
        ]


def _audit(session: Session, row: DuplicateRow, backup_file: Path) -> None:
    session.add(
        CorrectionAuditTrail(
            entity_type="rag_chunk",
            entity_id=row.source_row_id,
            field_name="row_deleted",
            raw_value=row.source_row_id,
            raw_source="rag_chunks:player_movements",
            corrected_value=row.natural_key,
            corrected_source="purge_duplicate_movement_chunks",
            correction_reason=row.reason,
            confidence=1.0,
            extra_metadata={"backup_file": str(backup_file), "natural_key": row.natural_key},
            created_at=datetime.now(KST),
            updated_at=datetime.now(KST),
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Plan or delete the duplicate legacy movement chunks.

    Args:
        argv: Command-line arguments.

    Returns:
        Exit code: 0 on success, 3 when the guard denies the write.

    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backup-out", type=Path, help="write the verified plan to this CSV (read-only)")
    parser.add_argument("--backup-file", type=Path, help="delete the rows listed in this CSV")
    parser.add_argument("--apply", action="store_true", help="actually delete (needs the env guard)")
    args = parser.parse_args(argv)

    if not args.backup_out and not args.backup_file:
        parser.error("one of --backup-out or --backup-file is required")

    if args.backup_out:
        with SessionLocal() as session:
            confirmed, rejected = plan_duplicates(session)
        _write_backup(args.backup_out, confirmed)
        print(f"verified {len(confirmed)} row(s) as duplicates → {args.backup_out}")
        if rejected:
            print(f"rejected {len(rejected)} row(s) — these stay:")
            for message in rejected[:10]:
                print(f"   {message}")
        return 0

    rows = load_backup(args.backup_file)
    if not args.apply or os.environ.get(ALLOW_ENV) != "1":
        print(f"plan: {len(rows)} row(s) would be deleted")
        for row in rows[:10]:
            print(f"   {row.source_row_id} → {row.natural_key}")
        if len(rows) > 10:
            print(f"   ... and {len(rows) - 10} more")
        print(f"\nRefusing to write: pass --apply and set {ALLOW_ENV}=1")
        return 3

    with SessionLocal() as session:
        ids = [row.chunk_id for row in rows]
        found = set(
            session.execute(
                select(RagChunk.id).where(RagChunk.id.in_(ids), or_(RagChunk.source_row_id.regexp_match("^id="))),
            ).scalars(),
        )
        if len(found) != len(ids):
            missing = sorted(set(ids) - found)
            message = f"refusing: {len(missing)} id(s) are missing or no longer legacy, e.g. {missing[:5]}"
            raise SystemExit(message)

        by_status: dict[str, list[int]] = {}
        for row in rows:
            _audit(session, row, args.backup_file)
            by_status.setdefault(row.index_status, []).append(row.chunk_id)

        retired = _retire_rows(session, by_status)
        session.commit()

    counts = Counter(row.index_status for row in rows)
    print(f"deleted {retired} row(s) from {dict(counts)}")
    print("audit rows written to correction_audit_trail (entity_type='rag_chunk', field_name='row_deleted')")
    return 0


if __name__ == "__main__":
    sys.exit(main())
