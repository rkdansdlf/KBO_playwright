"""Merge one duplicate ``game`` row into its recorded canonical row.

``20180801WOSK0`` (KH/SSG) and ``20180801NXSK0`` (NX/SK) are the same game --
same date, same franchises, same 8-14 score -- stored twice. The alias table
already maps the former onto the latter, so the application reads it as one
game, but the rows themselves are still split: the alias carries the play-by-play
(101 rows) while the canonical row carries the lineups, innings and box score.

Merging means moving the child rows to the canonical id and deleting the
duplicate ``game`` row. The alias row itself has to survive: ``game_id_aliases``
is keyed on ``alias_game_id`` with no foreign key to ``game``, while the
``canonical_game_id`` FK cascades -- so the canonical row must stay and the
duplicate must go.

Rows without a foreign key to ``game`` (``crawl_execution_runs``,
``correction_audit_trail``) are left alone. They are history: an execution ran,
a status was corrected, and rewriting the id would edit the record of what
happened rather than the game it happened to.

Dry run unless ``--apply`` is given; a JSON backup is written before any write.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from src.constants import KST
from src.db.engine import Engine

BACKUP_DIR = Path("data/repair_backups")

#: Child tables whose rows belong to the canonical game after the merge.
MOVED_TABLES = ("game_play_by_play", "game_events", "game_validation_metrics")

#: Child tables keyed on the game itself, where the canonical row already has one.
REPLACED_TABLES = ("game_metadata",)

#: Kept as they are: no foreign key to ``game``, and they record what happened.
PRESERVED_TABLES = ("crawl_execution_runs", "correction_audit_trail")


def _snapshot(alias_game_id: str) -> dict[str, list[dict]]:
    tables = ("game", *MOVED_TABLES, *REPLACED_TABLES, *PRESERVED_TABLES)
    snapshot: dict[str, list[dict]] = {}
    with Engine.connect() as conn:
        for table in tables:
            rows = conn.execute(
                text(f"SELECT * FROM {table} WHERE game_id = :game_id"),
                {"game_id": alias_game_id},
            ).mappings()
            snapshot[table] = [dict(row) for row in rows]
    return snapshot


def _counts(game_id: str, tables: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    with Engine.connect() as conn:
        for table in tables:
            counts[table] = conn.execute(
                text(f"SELECT count(*) FROM {table} WHERE game_id = :game_id"),
                {"game_id": game_id},
            ).scalar_one()
    return counts


def merge(alias_game_id: str, canonical_game_id: str, *, apply: bool) -> int:
    """Move the duplicate's children onto the canonical row and drop the duplicate.

    Args:
        alias_game_id: The duplicate game id that is leaving.
        canonical_game_id: The game id that keeps the game.
        apply: When False, only report what would change.

    Returns:
        Process exit code: 0 on success, 1 when the inputs are not a mergeable pair.

    """
    with Engine.connect() as conn:
        alias_row = conn.execute(
            text("SELECT game_id FROM game WHERE game_id = :game_id"),
            {"game_id": alias_game_id},
        ).scalar_one_or_none()
        canonical_row = conn.execute(
            text("SELECT game_id FROM game WHERE game_id = :game_id"),
            {"game_id": canonical_game_id},
        ).scalar_one_or_none()
        alias = conn.execute(
            text("SELECT canonical_game_id FROM game_id_aliases WHERE alias_game_id = :game_id"),
            {"game_id": alias_game_id},
        ).scalar_one_or_none()

    if alias_row is None or canonical_row is None:
        print(f"[refused] both rows must exist: alias={alias_row!r} canonical={canonical_row!r}")
        return 1
    if alias != canonical_game_id:
        print(f"[refused] {alias_game_id} is not aliased to {canonical_game_id} (found {alias!r})")
        return 1

    before_alias = _counts(alias_game_id, (*MOVED_TABLES, *REPLACED_TABLES))
    before_canonical = _counts(canonical_game_id, MOVED_TABLES)
    print(f"before: {alias_game_id} {before_alias} | {canonical_game_id} {before_canonical}")

    if not apply:
        print("[dry-run] no writes; pass --apply to merge")
        return 0

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    backup_path = BACKUP_DIR / f"game_merge_{alias_game_id}_{stamp}.json"
    snapshot = _snapshot(alias_game_id)
    backup_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print(f"backup: {backup_path} ({sum(len(v) for v in snapshot.values())} rows)")

    moved: dict[str, int] = {}
    with Engine.begin() as conn:
        for table in MOVED_TABLES:
            moved[table] = conn.execute(
                text(f"UPDATE {table} SET game_id = :canonical WHERE game_id = :alias"),
                {"canonical": canonical_game_id, "alias": alias_game_id},
            ).rowcount
        for table in REPLACED_TABLES:
            moved[table] = conn.execute(
                text(f"DELETE FROM {table} WHERE game_id = :alias"),
                {"alias": alias_game_id},
            ).rowcount
        moved["game"] = conn.execute(
            text("DELETE FROM game WHERE game_id = :alias"),
            {"alias": alias_game_id},
        ).rowcount

    after_alias = _counts(alias_game_id, (*MOVED_TABLES, *REPLACED_TABLES))
    after_canonical = _counts(canonical_game_id, MOVED_TABLES)
    with Engine.connect() as conn:
        alias_survives = conn.execute(
            text("SELECT canonical_game_id FROM game_id_aliases WHERE alias_game_id = :game_id"),
            {"game_id": alias_game_id},
        ).scalar_one_or_none()

    print(f"moved: {moved}")
    print(f"after: {alias_game_id} {after_alias} | {canonical_game_id} {after_canonical}")
    print(f"alias preserved: {alias_survives}")

    expected = {table: before_canonical[table] + before_alias[table] for table in MOVED_TABLES}
    if after_canonical != expected or any(after_alias.values()):
        print("[failed] the merge did not land where it was supposed to")
        return 1
    if alias_survives != canonical_game_id:
        print("[failed] the alias row did not survive the delete")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the merge for one duplicate pair."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alias-game-id", required=True)
    parser.add_argument("--canonical-game-id", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    return merge(args.alias_game_id, args.canonical_game_id, apply=args.apply)


if __name__ == "__main__":
    sys.exit(main())
