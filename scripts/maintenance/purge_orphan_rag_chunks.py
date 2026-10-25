"""Retire RAG chunks that no current generator can reproduce.

The store carries rows three different generators wrote under three different key
conventions. The consistency audit reports each of them as an ``embedding_missing``
finding because it can only see "this row has no embedding", and that is the whole
of what it can see. A missing embedding is not by itself a gap: for a row the
current generator can still produce, the fix is to re-embed it; for a row no
current generator can produce, there is no source to re-embed from, and writing a
vector under a key nothing else refers to only makes the row look healthy.

The three rules below name the second kind, and name why:

``source_table_not_indexed``
    The table is not one the build reads. ``game_metadata`` and ``game_summary``
    hold chunks with no source in ``_SOURCE_TABLE_BY_SOURCE`` -- they are outside
    the build's scope by design, not stale inside it. The set is read from the
    build at import rather than copied (see ``INDEXED_SOURCE_TABLES``), because a
    copied list fails in the one direction that matters: a source added to the
    build would stay "not indexed" here and its rows would be retired. That is
    not hypothetical -- an ``inning_scores`` source landed on 2026-10-26 while
    this script was being written, and the 5,391 inning rows that had been
    candidates became regenerable without any change here.
``absolute_source_path``
    The row was written by a deployment whose Markdown root was a container
    path (``/app/docs/kbo_knowledge/...``). The local loader builds keys from a
    root-relative path, so the same document under this repository produces a
    different key; the old key can never be regenerated here.
``legacy_key_convention``
    The row predates the current key shape. ``player_season_batting`` chunks are
    keyed ``{player}_{year}_{team}_{series}``; 1,534 rows are keyed ``id=1096``.

Only the first rule is allowed to speak about a table the build does not read.
Everything else requires a match, so a row whose key shape is still current is
left alone even when the generator skipped it -- 523 ``game_lineups`` rows have
no current counterpart, but asserting they are orphans would be a guess about
why the generator skipped them, and this script only writes what it can name.

Rows are retired through the lifecycle contract, never by assigning ``DELETED``.
``can_transition`` refuses ``ACTIVE -> DELETED`` precisely so a delete passes
through ``DELETE_PENDING``; the path is searched with ``can_transition`` itself
rather than a copied table, so it cannot drift from the contract. ``ACTIVE`` is
never used as a step on the way out: a row we are about to delete is not
currently serving, and saying it is would be the same false claim in the other
direction.

Usage::

    python3 -m scripts.maintenance.purge_orphan_rag_chunks            # dry run
    RAG_INDEX_ALLOW_WRITE=1 python3 -m scripts.maintenance.purge_orphan_rag_chunks --apply
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from src.cli.rag.build_rag_index import _SOURCE_TABLE_BY_SOURCE
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.rag_chunk import RagChunk
from src.services.rag_index_lifecycle import IndexLifecycleStatus, can_transition

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Source tables the index build actually reads. Derived from the build rather
#: than copied: a second list would let a source be added to the build and stay
#: "not indexed" here, which is the one direction that turns a live gap into a
#: silent delete.
INDEXED_SOURCE_TABLES: frozenset[str] = frozenset(_SOURCE_TABLE_BY_SOURCE.values())

#: Rows already off the search surface. ``DELETE_PENDING`` is deliberately *not*
#: here: it means "deletion was requested", and finishing that request is this
#: script's job.
TERMINAL_STATUSES: frozenset[str] = frozenset(
    {IndexLifecycleStatus.DELETED, IndexLifecycleStatus.TOMBSTONED, IndexLifecycleStatus.PURGED}
)

#: Lifecycle states this script will not route through. See the module docstring.
_NOT_A_ROUTE = frozenset({IndexLifecycleStatus.ACTIVE})

_LEGACY_KEY_PATTERN = re.compile(r"^id=\d+$")
_WINDOWS_ABSOLUTE_PATTERN = re.compile(r"^[A-Za-z]:[\\/]")

_BACKUP_COLUMNS = ("source_table", "source_row_id", "reason", "previous_status")


@dataclass(frozen=True, slots=True)
class OrphanReport:
    """Describe what the scan found and what the write did."""

    scanned: int = 0
    orphan_count: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)
    by_table: dict[str, int] = field(default_factory=dict)
    marked: int = 0
    already_terminal: int = 0
    backup_path: str | None = None

    def summary_line(self) -> str:
        """Return a one-line summary for logs."""
        return (
            f"scanned={self.scanned} orphans={self.orphan_count} marked={self.marked} "
            f"already_terminal={self.already_terminal} reasons={dict(self.by_reason)} "
            f"tables={dict(self.by_table)}"
        )


def _is_absolute_source_path(source_row_id: str) -> bool:
    """Return whether a key names an absolute filesystem path."""
    return (
        source_row_id.startswith("/")
        or source_row_id.startswith("\\\\")
        or bool(_WINDOWS_ABSOLUTE_PATTERN.match(source_row_id))
    )


def classify_unregenerable(source_table: str, source_row_id: str) -> str | None:
    """Return why no current generator can reproduce a row, or ``None``.

    Args:
        source_table: The chunk's ``source_table``.
        source_row_id: The chunk's identity within that table.

    Returns:
        A reason token, or ``None`` when the row may still have a source.

    """
    if source_table not in INDEXED_SOURCE_TABLES:
        return "source_table_not_indexed"
    if _is_absolute_source_path(source_row_id):
        return "absolute_source_path"
    if _LEGACY_KEY_PATTERN.match(source_row_id):
        return "legacy_key_convention"
    return None


def deletion_path(status: str) -> list[str] | None:
    """Return the allowed lifecycle steps from ``status`` to ``DELETED``.

    The path is searched with :func:`can_transition`, so every step is one the
    contract allows by construction.

    Args:
        status: The row's current ``index_status``.

    Returns:
        The steps to apply in order (``[]`` when there is nothing to do), or
        ``None`` when no allowed path exists.

    """
    if status in TERMINAL_STATUSES:
        return []
    target = IndexLifecycleStatus.DELETED

    def neighbours(current: str) -> list[str]:
        return [
            candidate
            for candidate in IndexLifecycleStatus
            if candidate not in _NOT_A_ROUTE and candidate != current and can_transition(current, candidate)
        ]

    parents: dict[str, str | None] = {status: None}
    queue: deque[str] = deque([status])
    while queue:
        current = queue.popleft()
        if current == target:
            path: list[str] = []
            node: str | None = current
            while node is not None and node != status:
                path.append(node)
                node = parents[node]
            return list(reversed(path))
        for candidate in neighbours(current):
            if candidate in parents:
                continue
            parents[candidate] = current
            queue.append(candidate)
    return None


def _write_backup(path: Path, rows: list[dict[str, str]]) -> str:
    """Write the orphan list to CSV before anything is changed.

    Args:
        path: Destination file.
        rows: Backup records, one per orphan.

    Returns:
        The written path as a string.

    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_BACKUP_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def _retire_rows(session: Session, ids_by_status: dict[str, list[int]]) -> int:
    """Walk each status group to ``DELETED`` one allowed step at a time.

    Grouped by starting status so one statement covers a whole step; every step
    is still taken, so the store observes the same chain the contract requires
    rather than a jump. The return value counts rows, not statements -- an
    ``ACTIVE`` row moves twice and is still one row.

    Args:
        session: Session the updates are executed through.
        ids_by_status: Row ids grouped by their current ``index_status``.

    Returns:
        The number of rows that were moved.

    """
    pending: dict[str, list[int]] = {}
    retired = 0
    for status, ids in ids_by_status.items():
        if not deletion_path(status):
            logger.error("no lifecycle path to DELETED from %s; %d row(s) left alone", status, len(ids))
            continue
        pending[status] = list(ids)
        retired += len(ids)

    while pending:
        next_pending: dict[str, list[int]] = {}
        for status, ids in pending.items():
            path = deletion_path(status) or []
            if not path:
                continue
            step = path[0]
            session.execute(
                update(RagChunk)
                .where(RagChunk.id.in_(ids))
                .values(index_status=step)
                .execution_options(synchronize_session=False),
            )
            if len(path) > 1:
                next_pending.setdefault(step, []).extend(ids)
        pending = next_pending
    return retired


def purge_unregenerable(
    session: Session,
    *,
    apply: bool,
    backup_path: Path | str | None = None,
) -> OrphanReport:
    """Retire rows no current generator can reproduce.

    The function owns its transaction: the write is committed when ``apply`` is
    true, because a repair that only calls ``add`` or ``execute`` reports success
    and writes nothing.

    Args:
        session: Session the rows are read and written through.
        apply: Write the lifecycle transitions. When false nothing changes.
        backup_path: Optional CSV path for the pre-change state. Written only on
            an apply, and written before the first row changes.

    Returns:
        What was found and what was done.

    """
    # Project the columns rather than loading rows: ``content`` is the bulk of the
    # table and none of it is needed to decide or to write.
    rows = session.execute(
        select(
            RagChunk.id,
            RagChunk.source_table,
            RagChunk.source_row_id,
            RagChunk.index_status,
        ).where(RagChunk.embedding.is_(None)),
    ).all()

    by_reason: Counter[str] = Counter()
    by_table: Counter[str] = Counter()
    ids_by_status: dict[str, list[int]] = {}
    backup_rows: list[dict[str, str]] = []
    already_terminal = 0

    for chunk_id, source_table, source_row_id, index_status in rows:
        reason = classify_unregenerable(str(source_table), str(source_row_id))
        if reason is None:
            continue
        if index_status in TERMINAL_STATUSES:
            already_terminal += 1
            continue
        by_reason[reason] += 1
        by_table[str(source_table)] += 1
        ids_by_status.setdefault(str(index_status), []).append(int(chunk_id))
        backup_rows.append(
            {
                "source_table": str(source_table),
                "source_row_id": str(source_row_id),
                "reason": reason,
                "previous_status": str(index_status),
            },
        )

    written_backup: str | None = None
    marked = 0
    if apply and backup_rows:
        if backup_path is not None:
            written_backup = _write_backup(Path(backup_path), backup_rows)
        marked = _retire_rows(session, ids_by_status)
        session.commit()

    return OrphanReport(
        scanned=len(rows),
        orphan_count=len(backup_rows),
        by_reason=dict(by_reason),
        by_table=dict(by_table),
        marked=marked,
        already_terminal=already_terminal,
        backup_path=written_backup,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the purge from the command line.

    Args:
        argv: Optional argument list.

    Returns:
        Process exit code: 0 on success, 3 when the write guard refuses.

    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--apply", action="store_true", help="Write the lifecycle transitions (default: dry run)")
    parser.add_argument(
        "--backup-out",
        type=Path,
        default=None,
        help="CSV path for the pre-change state (default: data/repair_backups/rag_orphans_<UTC>.csv)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.apply and os.getenv("RAG_INDEX_ALLOW_WRITE") != "1":
        logger.error("refusing to write: pass --apply with RAG_INDEX_ALLOW_WRITE=1")
        return 3

    backup_path = args.backup_out
    if args.apply and backup_path is None:
        stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
        backup_path = Path("data") / "repair_backups" / f"rag_orphans_{stamp}.csv"

    with SessionLocal() as session:
        report = purge_unregenerable(session, apply=args.apply, backup_path=backup_path)

    logger.info("%s", report.summary_line())
    if report.backup_path:
        logger.info("backup written to %s", report.backup_path)
    if not args.apply:
        logger.info("dry run; pass --apply with RAG_INDEX_ALLOW_WRITE=1 to write")
    return 0


if __name__ == "__main__":
    sys.exit(main())
