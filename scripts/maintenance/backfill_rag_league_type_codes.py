"""Re-derive ``rag_chunks.league_type_code`` from the season each row belongs to.

The field is what a search filters on to tell a Korean Series game from a
regular-season one, and the game-level generators wrote the regular-season
constant for every row. A postseason chunk was therefore indistinguishable from
a regular-season one and ``WHERE league_type_code = 5`` returned nothing at all.
The generators are fixed, but the rows already written still carry the constant.

This is a metadata repair, not a re-index. ``content_hash`` is built from the
chunk's title and content and does not include the league code, so the embeddings
still describe their rows exactly; re-embedding 120,000 chunks to change one
filter column would spend the budget for nothing, and deleting and re-inserting
them would pass through ``DELETED``, which the incremental selector never
reactivates. The values are updated in place.

Two rules, and they are different in kind:

``game_scoped``
    ``game``, ``game_lineups``, ``game_inning_scores``, ``game_highlights`` and
    ``game_play_by_play`` derive the code from ``game.season_id`` ->
    ``kbo_seasons.league_type_code``. The game is read from ``meta.game_id``,
    and for ``game`` rows the key is the game id. A row whose game cannot be
    found is left alone rather than being written as 0 -- "I do not know" and
    "this is the regular season" are different claims, and the second one is
    what a filter acts on.
``teams``
    Team identity is not season-scoped, so it has no league type to report. It
    is set to NULL rather than 0 for the same reason.

Rows that already carry the right value are not touched, so the run is
idempotent and its report shows zero remaining candidates on a second pass.

Usage::

    python3 -m scripts.maintenance.backfill_rag_league_type_codes            # dry run
    RAG_INDEX_ALLOW_WRITE=1 python3 -m scripts.maintenance.backfill_rag_league_type_codes --apply
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select, update

from src.cli.rag.build_rag_index import _game_season_codes
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.rag_chunk import RagChunk

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Sources whose rows are one game and can be given that game's season type.
GAME_SCOPED_SOURCES: frozenset[str] = frozenset(
    {
        "game",
        "game_lineups",
        "game_inning_scores",
        "game_highlights",
        "game_play_by_play",
    },
)

#: Sources with no season scope at all: the value is cleared instead of set.
SEASONLESS_SOURCES: frozenset[str] = frozenset({"teams"})

#: Chunk ids per UPDATE statement. The whole set is a few hundred thousand rows
#: and a single statement would hold one transaction for all of it.
_BATCH_SIZE = 5_000

_BACKUP_COLUMNS = ("chunk_id", "source_table", "source_row_id", "previous_code", "desired_code")


@dataclass(frozen=True, slots=True)
class BackfillReport:
    """Describe what the scan found and what the write changed."""

    scanned: int = 0
    pending: int = 0
    updated: int = 0
    unknown_game: int = 0
    by_source: dict[str, int] = field(default_factory=dict)
    backup_path: str | None = None

    def summary_line(self) -> str:
        """Return a one-line summary for logs."""
        return (
            f"scanned={self.scanned} pending={self.pending} updated={self.updated} "
            f"unknown_game={self.unknown_game} sources={dict(self.by_source)}"
        )


def game_id_for(source_table: str, source_row_id: str, meta: object) -> str | None:
    """Return the game a chunk belongs to, or ``None`` when it cannot be read.

    ``game`` rows key on the game itself. The others record it in their metadata,
    which is where the generator put it; the key shape differs per source
    (``20181102NXSK0_BIG_PLAY_116``, ``20180324HHWO0_content_20f49ee12f83``), so
    splitting it would be a guess at a format this function does not own.

    Args:
        source_table: The chunk's ``source_table``.
        source_row_id: The chunk's identity within that table.
        meta: The chunk's metadata, already deserialised.

    Returns:
        The game id, or ``None``.

    """
    if source_table == "game":
        return source_row_id
    if isinstance(meta, Mapping):
        value = meta.get("game_id")
        if value:
            return str(value)
    return None


def plan_codes(
    rows: Sequence[tuple[int, str, str, int | None, object]],
    season_codes: Mapping[str, int],
) -> list[tuple[int, int | None, int | None]]:
    """Return the rows whose stored code disagrees with the one they should have.

    Args:
        rows: ``(id, source_table, source_row_id, league_type_code, meta)``.
        season_codes: ``game_id -> league_type_code`` for every stored game.

    Returns:
        ``(chunk_id, current, desired)`` for each row that needs a write. A game
        the map does not hold is reported by the caller as unknown, not written.

    """
    planned: list[tuple[int, int | None, int | None]] = []
    for chunk_id, source_table, source_row_id, current, meta in rows:
        if source_table in SEASONLESS_SOURCES:
            desired: int | None = None
        else:
            game_id = game_id_for(source_table, source_row_id, meta)
            if game_id is None or game_id not in season_codes:
                continue
            desired = season_codes[game_id]
        if current != desired:
            planned.append((int(chunk_id), current, desired))
    return planned


def _write_backup(path: Path, rows: list[dict[str, Any]]) -> str:
    """Write the pre-change state to CSV before anything is modified."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_BACKUP_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def backfill_league_type_codes(
    session: Session,
    *,
    apply: bool,
    backup_path: Path | str | None = None,
) -> BackfillReport:
    """Rewrite ``league_type_code`` for the rows the generators wrote wrong.

    Args:
        session: Session the rows are read and written through.
        apply: Write the updates. When false nothing changes.
        backup_path: Optional CSV recording the pre-change state.

    Returns:
        What was found and what was written.

    """
    sources = sorted(GAME_SCOPED_SOURCES | SEASONLESS_SOURCES)
    rows = session.execute(
        select(
            RagChunk.id,
            RagChunk.source_table,
            RagChunk.source_row_id,
            RagChunk.league_type_code,
            RagChunk.meta,
        ).where(RagChunk.source_table.in_(sources)),
    ).all()
    season_codes = _game_season_codes(session)

    planned = plan_codes(
        [(int(r[0]), str(r[1]), str(r[2]), r[3], r[4]) for r in rows],
        season_codes,
    )
    unknown = sum(
        1
        for _chunk_id, source_table, source_row_id, _current, meta in rows
        if source_table not in SEASONLESS_SOURCES
        and (game_id := game_id_for(str(source_table), str(source_row_id), meta)) is not None
        and game_id not in season_codes
    )
    by_source = Counter(str(row[1]) for row in rows if row[1] in GAME_SCOPED_SOURCES)

    written_backup: str | None = None
    updated = 0
    if apply and planned:
        if backup_path is not None:
            by_id = {int(r[0]): r for r in rows}
            written_backup = _write_backup(
                Path(backup_path),
                [
                    {
                        "chunk_id": chunk_id,
                        "source_table": str(by_id[chunk_id][1]),
                        "source_row_id": str(by_id[chunk_id][2]),
                        "previous_code": "" if current is None else current,
                        "desired_code": "" if desired is None else desired,
                    }
                    for chunk_id, current, desired in planned
                ],
            )
        grouped: dict[int | None, list[int]] = {}
        for chunk_id, _current, desired in planned:
            grouped.setdefault(desired, []).append(chunk_id)
        for desired, ids in grouped.items():
            for start in range(0, len(ids), _BATCH_SIZE):
                batch = ids[start : start + _BATCH_SIZE]
                session.execute(
                    update(RagChunk)
                    .where(RagChunk.id.in_(batch))
                    .values(league_type_code=desired)
                    .execution_options(synchronize_session=False),
                )
                updated += len(batch)
        session.commit()

    return BackfillReport(
        scanned=len(rows),
        pending=len(planned),
        updated=updated,
        unknown_game=unknown,
        by_source=dict(by_source),
        backup_path=written_backup,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the backfill from the command line.

    Args:
        argv: Optional argument list.

    Returns:
        Process exit code: 0 on success, 3 when the write guard refuses.

    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--apply", action="store_true", help="Write the codes (default: dry run)")
    parser.add_argument(
        "--backup-out",
        type=Path,
        default=None,
        help="CSV path for the pre-change state (default: data/repair_backups/rag_league_codes_<UTC>.csv)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.apply and os.getenv("RAG_INDEX_ALLOW_WRITE") != "1":
        logger.error("refusing to write: pass --apply with RAG_INDEX_ALLOW_WRITE=1")
        return 3

    backup_path = args.backup_out
    if args.apply and backup_path is None:
        stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
        backup_path = Path("data") / "repair_backups" / f"rag_league_codes_{stamp}.csv"

    with SessionLocal() as session:
        report = backfill_league_type_codes(session, apply=args.apply, backup_path=backup_path)

    logger.info("%s", report.summary_line())
    if report.backup_path:
        logger.info("backup written to %s", report.backup_path)
    if not args.apply:
        logger.info("dry run; pass --apply with RAG_INDEX_ALLOW_WRITE=1 to write")
    return 0


if __name__ == "__main__":
    sys.exit(main())
