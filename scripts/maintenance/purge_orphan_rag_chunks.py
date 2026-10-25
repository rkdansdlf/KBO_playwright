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
``source_rows_absent``
    The key is the shape the generator still produces, but the source table holds
    no row for it. 523 ``game_lineups`` chunks name games with no lineup rows at
    all -- the only rule whose evidence has to be measured rather than read off
    the key, which is why it lives in :func:`absent_source_identity` instead of
    :func:`classify_unregenerable`.

Only the first rule is allowed to speak about a table the build does not read.
Everything else requires a match. ``source_rows_absent`` is the narrow exception
that was earned rather than assumed: 523 ``game_lineups`` rows were first left
alone because a current key shape is not proof of an orphan, and only after the
source table was counted -- 261 of 262 games hold no lineup row -- was the rule
added. It is registered per table (``_SOURCE_KEY_RULES``), because the same
argument cannot be made for a table whose key has no registered parser, and an
empty source table answers "nothing is loaded" rather than "nothing exists".

**Being out of the build's scope is not the same claim as being unserved.** The
scan reads ``embedding IS NULL`` and that predicate, not the rules, is what keeps
this safe: ``game_summary`` holds 68,829 embedded rows whose keys all match the
legacy ``id=<n>`` shape against a table no source builds, so both the first and
the third rule would name every one of them. They are served, they answer
queries, and deleting them would be a large silent regression dressed as
housekeeping. Two claims are needed to justify a delete -- "no current generator
can reproduce this" and "nothing is being served from it" -- and the rules only
establish the first. A row with an embedding is never a candidate here, however
convincing its key shape looks.

The converse is why the rules cannot be written as a checklist either: on
2026-10-26 the ``game_inning_scores`` rows had no source and no embedding, and
would have been retired as out-of-scope; hours later an ``inning_scores`` source
was added to the build and the same 5,391 rows became regenerable. A judgement
about what a table is *for* expires when the deployment changes, so the rule set
is derived at import and the scan is limited to rows that are already invisible.

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
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, update

from src.cli.rag.build_rag_index import _SOURCE_TABLE_BY_SOURCE
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import GameLineup
from src.models.rag_chunk import RagChunk
from src.services.rag_index_lifecycle import IndexLifecycleStatus, can_transition

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

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

#: The team sides a lineup key can name. Read as a closed set so a key that does
#: not carry one is a parse failure rather than a game whose side is unknown.
_LINEUP_SIDES = frozenset({"home", "away"})

_BACKUP_COLUMNS = ("source_table", "source_row_id", "reason", "previous_status")


def _game_id_from_lineup_key(key: str) -> str | None:
    """Return the game a lineup chunk key names, or ``None`` when it cannot be read.

    ``_iter_lineup_chunks`` writes ``{game_id}_{team_side}``. The side is checked
    against the two values the generator emits, so a key of another shape fails
    the parse instead of contributing a game id guessed from its prefix.

    Args:
        key: The chunk's ``source_row_id``.

    Returns:
        The game id, or ``None``.

    """
    game_id, separator, side = key.rpartition("_")
    if not separator or not game_id or side not in _LINEUP_SIDES:
        return None
    return game_id


def _lineup_source_presence(session: Session, identities: set[str]) -> set[str] | None:
    """Return which game ids hold at least one lineup row.

    Args:
        session: Session the source table is read through.
        identities: Game ids the candidate keys name.

    Returns:
        The ids that have rows, or ``None`` when the question cannot be answered
        because the table holds nothing at all.

    """
    if not session.execute(select(func.count()).select_from(GameLineup)).scalar_one():
        # An empty table says "nothing is loaded", not "no game has lineups".
        # Reading it as absence would retire every lineup chunk in one run and
        # report it as housekeeping.
        return None
    rows = session.execute(
        select(GameLineup.game_id).where(GameLineup.game_id.in_(identities)).distinct(),
    ).scalars()
    return {str(game_id) for game_id in rows}


@dataclass(frozen=True, slots=True)
class SourceKeyRule:
    """How a chunk key is read back to a source identity, and how it is checked.

    Attributes:
        parse_key: Turn a chunk key into the source-table identity it names, or
            ``None`` when the key cannot be read.
        presence: Return the subset of identities the source table holds, or
            ``None`` when the table cannot answer.

    """

    parse_key: Callable[[str], str | None]
    presence: Callable[[Session, set[str]], set[str] | None]


#: Tables eligible for the ``source_rows_absent`` rule. Absence is asserted only
#: where a key can be read back to a source identity that can then be looked up;
#: without an entry the key would have to be guessed at, and a guess on this path
#: writes deletes.
_SOURCE_KEY_RULES: dict[str, SourceKeyRule] = {
    "game_lineups": SourceKeyRule(parse_key=_game_id_from_lineup_key, presence=_lineup_source_presence),
}


@dataclass(frozen=True, slots=True)
class _ScanResult:
    """The rows a purge would act on, grouped the way the write needs them."""

    ids_by_status: dict[str, list[int]] = field(default_factory=dict)
    backup_rows: list[dict[str, str]] = field(default_factory=list)
    by_reason: dict[str, int] = field(default_factory=dict)
    by_table: dict[str, int] = field(default_factory=dict)
    already_terminal: int = 0


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


def absent_source_identity(
    session: Session,
    source_table: str,
    source_row_ids: Iterable[str],
) -> frozenset[str]:
    """Return the chunk keys whose source table holds no matching row.

    This is the one rule that cannot be read off the key: ``game_lineups`` keys
    have the shape the generator still produces, so shape alone says nothing.
    523 of them name games that hold no lineup rows at all, and no amount of
    re-embedding can give them a source. The check is measured against the
    source table rather than inferred.

    Absence is decided per source identity, not per key. A game with only the
    away side stored has lineups; its home chunk may simply not have been
    produced, and retiring it would be asserting a fact about the generator that
    the source does not show.

    Args:
        session: Session the source table is read through.
        source_table: The chunk's ``source_table``.
        source_row_ids: Candidate keys, typically the whole scanned batch.

    Returns:
        The keys whose source identity is missing. Empty when the table has no
        registered rule, or when the question cannot be answered.

    """
    rule = _SOURCE_KEY_RULES.get(source_table)
    if rule is None:
        return frozenset()

    by_identity: dict[str, list[str]] = {}
    for raw_key in source_row_ids:
        key = str(raw_key)
        identity = rule.parse_key(key)
        if identity is None:
            continue
        by_identity.setdefault(identity, []).append(key)
    if not by_identity:
        return frozenset()

    present = rule.presence(session, set(by_identity))
    if present is None:
        return frozenset()

    return frozenset(key for identity, keys in by_identity.items() if identity not in present for key in keys)


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


def _absent_keys_by_table(session: Session, rows: Sequence[Any]) -> set[tuple[str, str]]:
    """Resolve source absence for a whole batch in one probe per table.

    Absence is a property of the source table, not of the row, so the question is
    asked once per table over every candidate key rather than once per row. The
    answer is keyed by table because two tables can carry the same key text.

    Args:
        session: Session the candidate rows were read through.
        rows: The scanned ``(id, source_table, source_row_id, status)`` tuples.

    Returns:
        ``(source_table, source_row_id)`` pairs whose source identity is missing.

    """
    keys_by_table: dict[str, list[str]] = {}
    for _chunk_id, source_table, source_row_id, _status in rows:
        keys_by_table.setdefault(str(source_table), []).append(str(source_row_id))

    absent: set[tuple[str, str]] = set()
    for table, keys in keys_by_table.items():
        for key in absent_source_identity(session, table, keys):
            absent.add((table, key))
    return absent


def _classify_rows(rows: Sequence[Any], absent: set[tuple[str, str]]) -> _ScanResult:
    """Name a reason for every scanned row that has one.

    Args:
        rows: The scanned ``(id, source_table, source_row_id, status)`` tuples.
        absent: ``(table, key)`` pairs whose source identity is missing.

    Returns:
        The rows a purge would act on, grouped for the write.

    """
    by_reason: Counter[str] = Counter()
    by_table: Counter[str] = Counter()
    ids_by_status: dict[str, list[int]] = {}
    backup_rows: list[dict[str, str]] = []
    already_terminal = 0

    for chunk_id, source_table, source_row_id, index_status in rows:
        table = str(source_table)
        key = str(source_row_id)
        reason = classify_unregenerable(table, key)
        if reason is None and (table, key) in absent:
            reason = "source_rows_absent"
        if reason is None:
            continue
        if index_status in TERMINAL_STATUSES:
            already_terminal += 1
            continue
        by_reason[reason] += 1
        by_table[table] += 1
        ids_by_status.setdefault(str(index_status), []).append(int(chunk_id))
        backup_rows.append(
            {
                "source_table": table,
                "source_row_id": key,
                "reason": reason,
                "previous_status": str(index_status),
            },
        )

    return _ScanResult(
        ids_by_status=ids_by_status,
        backup_rows=backup_rows,
        by_reason=dict(by_reason),
        by_table=dict(by_table),
        already_terminal=already_terminal,
    )


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
    # table and none of it is needed to decide or to write. The predicate is what
    # keeps the rules inert against served rows -- ``game_summary`` would match
    # two of them and holds 68,829 embedded rows.
    rows = session.execute(
        select(
            RagChunk.id,
            RagChunk.source_table,
            RagChunk.source_row_id,
            RagChunk.index_status,
        ).where(RagChunk.embedding.is_(None)),
    ).all()

    scan = _classify_rows(rows, _absent_keys_by_table(session, rows))

    written_backup: str | None = None
    marked = 0
    if apply and scan.backup_rows:
        if backup_path is not None:
            written_backup = _write_backup(Path(backup_path), scan.backup_rows)
        marked = _retire_rows(session, scan.ids_by_status)
        session.commit()

    return OrphanReport(
        scanned=len(rows),
        orphan_count=len(scan.backup_rows),
        by_reason=scan.by_reason,
        by_table=scan.by_table,
        marked=marked,
        already_terminal=scan.already_terminal,
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
