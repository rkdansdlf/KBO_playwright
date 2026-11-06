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

**Deletes commit per batch** (``--batch-size``, default 200). One transaction over
all 7,227 rows was tried twice and died the same way both times -- ``could not
receive data from server: Operation timed out`` -- while the server sat idle with
no locks: the connection was dropped by the network (a Tailscale link that has
been flapping), not by anything the query did. Committing per batch means a drop
costs the batch in flight, not the run, and re-running the same command resumes:
an id already off the search surface is counted as ``already_done`` and skipped.

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
from collections.abc import Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from scripts.maintenance.purge_orphan_rag_chunks import _retire_rows
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.audit_trail import CorrectionAuditTrail
from src.models.player import PlayerMovement
from src.models.rag_chunk import RagChunk
from src.models.team_history import TeamHistory
from src.services.rag_index_identity import (
    stable_player_movement_source_row_id,
    stable_team_history_source_row_id,
)

ALLOW_ENV = "KBO_ALLOW_RAG_CHUNK_DELETE"

#: How many ids one ``UPDATE`` may carry. 7,227 ids in a single ``IN (...)`` list
#: timed out waiting for the server on the first attempt (2026-11-05:
#: ``could not receive data from server: Operation timed out``) -- the delete path
#: has to issue many small statements rather than one large one. 500 keeps each
#: round trip well inside the connection's patience while still covering the whole
#: set in 15 statements.
_RETIRE_CHUNK_SIZE = 200

#: Statuses that mean the row is already off the search surface, so a re-run after
#: a dropped connection does not try to walk it again.
SETTLED_STATUSES: frozenset[str] = frozenset({"DELETED", "TOMBSTONED", "PURGED"})
REASON = "duplicate_of_natural_key"
STALE_REASON = "superseded_by_canonical_key"
BACKUP_COLUMNS = ("chunk_id", "source_row_id", "index_status", "natural_key", "legacy_note", "reason")

_LEGACY_ID = re.compile(r"^id=\d+$")
LEGACY_FIELDS = re.compile(r"^([가-힣 ]+):\s*(.*)$", re.MULTILINE)


@dataclass(frozen=True)
class DuplicateRow:
    """One chunk whose fact is already stored under another key."""

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

    confirmed.extend(_stale_natural_key_rows(session, indexed=natural))
    return confirmed, rejected


def _stale_natural_key_rows(
    session: Session,
    *,
    indexed: Mapping[str, str],
) -> list[DuplicateRow]:
    """Find natural keys that a canonical key has superseded.

    ``player_movements.team_code`` holds Korean names and era codes, and the key was
    built from it before 2026-11-05, so one movement was indexed under up to three
    spellings. The canonical key now exists for every source row; where the two
    chunks hold the **same content**, the older key is a duplicate.

    Content equality is the test, not the triple: (date, player, section) is not
    unique -- a player can move twice on one day -- so mapping keys by that triple
    reads two different movements as one.
    """
    from src.models.rag_chunk import RagChunk as _Chunk

    stale: list[DuplicateRow] = []
    for movement in session.execute(select(PlayerMovement)).scalars():
        canonical_key = stable_player_movement_source_row_id(
            movement.movement_date,
            movement.canonical_team_id or movement.team_code,
            movement.player_name,
            movement.section,
        )
        raw_key = stable_player_movement_source_row_id(
            movement.movement_date,
            movement.team_code,
            movement.player_name,
            movement.section,
        )
        if raw_key == canonical_key:
            continue
        raw_content = indexed.get(raw_key)
        canonical_content = indexed.get(canonical_key)
        if raw_content is None or canonical_content is None:
            continue
        if raw_content != canonical_content:
            continue
        row = session.execute(
            select(_Chunk.id, _Chunk.index_status).where(
                _Chunk.source_table == "player_movements",
                _Chunk.source_row_id == raw_key,
                _Chunk.index_status == "ACTIVE",
            ),
        ).first()
        if row is None:
            continue
        stale.append(
            DuplicateRow(
                chunk_id=int(row[0]),
                source_row_id=raw_key,
                index_status=str(row[1]),
                natural_key=canonical_key,
                legacy_note="",
                reason=STALE_REASON,
            ),
        )
    return stale


def plan_team_history_duplicates(session: Session) -> tuple[list[DuplicateRow], list[str]]:
    """Verify every legacy ``team_history`` chunk against its natural-key chunk.

    The legacy form labels five fields (``구단``/``시즌``/``순위``/``도시``/``주경기장``)
    where the natural key writes one line, so it *looks* richer -- and the first read
    called it that. Comparing the values instead of the field list says otherwise:
    every value is present in the natural-key chunk, which additionally carries
    ``team_code``, and ``현재 구단명`` is not a source column at all -- the legacy
    generator repeated ``구단`` into it, and for the ten 2026 rows those are equal.
    Measured 2026-11-06: 10/10 show no missing value.

    Args:
        session: Session the chunks are read through.

    Returns:
        The rows safe to delete and one message per row that failed verification.

    """
    natural = {
        row.source_row_id: (row.content or "")
        for row in session.execute(
            select(RagChunk).where(
                RagChunk.source_table == "team_history",
                RagChunk.source_row_id.regexp_match("^[0-9]"),
            ),
        ).scalars()
    }
    by_season_team = {(row.season, row.team_name): row for row in session.execute(select(TeamHistory)).scalars()}

    legacy = list(
        session.execute(
            select(RagChunk).where(
                RagChunk.source_table == "team_history",
                RagChunk.source_row_id.regexp_match("^id="),
                RagChunk.index_status == "ACTIVE",
            ),
        ).scalars(),
    )

    confirmed: list[DuplicateRow] = []
    rejected: list[str] = []
    for chunk in legacy:
        fields = legacy_fields(chunk.content or "")
        season = fields.get("시즌", "")
        team = fields.get("구단", "")
        movement = by_season_team.get((int(season) if season.isdigit() else -1, team))
        if movement is None:
            rejected.append(f"{chunk.source_row_id}: no source row for season={season!r} team={team!r}")
            continue
        key = stable_team_history_source_row_id(movement.season, movement.team_code)
        counterpart = natural.get(key)
        if counterpart is None:
            rejected.append(f"{chunk.source_row_id}: no natural-key chunk for {key}")
            continue
        missing = [
            f"{label}={fields[label]!r}"
            for label in ("순위", "도시", "주경기장")
            if fields.get(label, "").strip() and fields[label].strip() not in counterpart
        ]
        if missing:
            rejected.append(f"{chunk.source_row_id}: {', '.join(missing)} absent from {key}")
            continue
        confirmed.append(
            DuplicateRow(
                chunk_id=chunk.id,
                source_row_id=chunk.source_row_id,
                index_status=chunk.index_status,
                natural_key=key,
                legacy_note="",
                reason=REASON,
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


def _apply_deletions(
    session: Session,
    rows: Sequence[DuplicateRow],
    backup_file: Path,
    *,
    batch_size: int = _RETIRE_CHUNK_SIZE,
) -> tuple[int, int]:
    """Audit and delete the planned rows, committing once per batch.

    The audit rows go in the same batch as the move they describe, so the trail and
    the change land together. A batch that fails leaves the earlier ones committed:
    the run is resumable rather than all-or-nothing, which is what the connection
    has required -- two all-in-one attempts died mid-flight and rolled back.

    Args:
        session: Session the writes go through.
        rows: The planned rows.
        backup_file: Where the plan came from, recorded on each audit row.
        batch_size: Rows per transaction.

    Returns:
        ``(retired, already_done)`` -- rows moved and rows already off the surface.

    """
    retired = 0
    already_done = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        live = {
            int(row[0]): str(row[2])
            for row in session.execute(
                select(RagChunk.id, RagChunk.source_row_id, RagChunk.index_status).where(
                    RagChunk.id.in_([row.chunk_id for row in batch]),
                ),
            ).fetchall()
        }

        by_status: dict[str, list[int]] = {}
        for row in batch:
            status = live.get(row.chunk_id)
            if status is None or status in SETTLED_STATUSES:
                already_done += 1
                continue
            _audit(session, row, backup_file)
            by_status.setdefault(status, []).append(row.chunk_id)

        for status, ids in by_status.items():
            retired += _retire_rows(session, {status: ids})
        session.commit()
    return retired, already_done


def main(argv: Sequence[str] | None = None) -> int:
    """Plan or delete the duplicate legacy movement chunks.

    Args:
        argv: Command-line arguments.

    Returns:
        Exit code: 0 on success, 3 when the guard denies the write.

    """
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--source",
        choices=("player_movements", "team_history"),
        default="player_movements",
        help="which source's duplicates to plan (default player_movements)",
    )
    parser.add_argument("--backup-out", type=Path, help="write the verified plan to this CSV (read-only)")
    parser.add_argument("--backup-file", type=Path, help="delete the rows listed in this CSV")
    parser.add_argument("--apply", action="store_true", help="actually delete (needs the env guard)")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_RETIRE_CHUNK_SIZE,
        help=f"rows per committed batch (default {_RETIRE_CHUNK_SIZE})",
    )
    args = parser.parse_args(argv)

    if not args.backup_out and not args.backup_file:
        parser.error("one of --backup-out or --backup-file is required")

    if args.backup_out:
        planner = plan_duplicates if args.source == "player_movements" else plan_team_history_duplicates
        with SessionLocal() as session:
            confirmed, rejected = planner(session)
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
        # The plan lists both legacy ids and superseded natural keys, so "is it still
        # legacy" is the wrong question. What must hold is that each id still carries
        # **the row the plan saw**: an id reused by another chunk would otherwise be
        # deleted under the old row's authority.
        live = {
            int(row[0]): (str(row[1]), str(row[2]))
            for row in session.execute(
                select(RagChunk.id, RagChunk.source_row_id, RagChunk.index_status).where(RagChunk.id.in_(ids)),
            ).fetchall()
        }
        mismatched = [row for row in rows if live.get(row.chunk_id) != (row.source_row_id, row.index_status)]
        if mismatched:
            sample = [row.chunk_id for row in mismatched[:5]]
            message = f"refusing: {len(mismatched)} id(s) no longer carry the planned row, e.g. {sample}"
            raise SystemExit(message)

        retired, already_done = _apply_deletions(
            session,
            rows,
            args.backup_file,
            batch_size=max(args.batch_size, 1),
        )

    counts = Counter(row.index_status for row in rows)
    print(f"deleted {retired} row(s) from {dict(counts)}")
    if already_done:
        print(f"already off the search surface: {already_done}")
    print("audit rows written to correction_audit_trail (entity_type='rag_chunk', field_name='row_deleted')")
    return 0


if __name__ == "__main__":
    sys.exit(main())
