"""Merge duplicate player identities (legacy <10000 -> modern >=50000).

For every *clean* pair (exactly one legacy id + one modern id sharing a name),
re-point all references from the legacy id to the modern canonical id, then drop
the legacy stub. Where a unique-key collision would occur (the modern id already
has a row for the same game/season), the legacy row is dropped and the modern row
is retained (no data loss, modern is the canonical record).

Read-only by default (``--dry-run``); pass ``--apply`` to write. Limit with
``--limit N`` to process only the first N pairs (useful for a smoke test).
"""

from __future__ import annotations

import argparse
import json
import os
from sqlalchemy import bindparam, create_engine, text

from scripts.maintenance.audit_player_duplicates import audit_player_duplicates

TABLES: dict[str, list[str]] = {
    "player_game_batting": ["game_id"],
    "player_game_pitching": ["game_id"],
    "player_season_batting": ["season", "league", "level"],
    "player_season_pitching": ["season", "league", "level"],
    "game_batting_stats": ["game_id", "appearance_seq"],
    "game_pitching_stats": ["game_id", "appearance_seq"],
    "game_lineups": [],
    "player_identities": [],
}


def _merge_one_pair(conn, legacy_id: int, modern_id: int, apply: bool) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, key_cols in TABLES.items():
        if key_cols:
            join = " AND ".join(f"M.{k} = L.{k}" for k in key_cols)
            collision_sql = (
                f"SELECT L.id FROM {table} L "
                f"JOIN {table} M ON M.player_id = :modern AND {join} "
                f"WHERE L.player_id = :legacy"
            )
            collision_ids = [
                r[0] for r in conn.execute(text(collision_sql), {"modern": modern_id, "legacy": legacy_id})
            ]
            repoint_sql = f"SELECT id FROM {table} WHERE player_id = :legacy"
            repoint_ids = [r[0] for r in conn.execute(text(repoint_sql), {"legacy": legacy_id})]
            repoint_ids = [i for i in repoint_ids if i not in set(collision_ids)]
            repoint_n = len(repoint_ids)
            collision_n = len(collision_ids)
        else:
            repoint_sql = f"SELECT id FROM {table} WHERE player_id = :legacy"
            repoint_ids = [r[0] for r in conn.execute(text(repoint_sql), {"legacy": legacy_id})]
            repoint_n = len(repoint_ids)
            collision_n = 0
            collision_ids = []
        if apply:
            if repoint_ids:
                conn.execute(
                    text(f"UPDATE {table} SET player_id = :modern WHERE id IN :ids").bindparams(
                        bindparam("ids", expanding=True)
                    ),
                    {"modern": modern_id, "ids": repoint_ids},
                )
            if collision_ids:
                conn.execute(
                    text(f"DELETE FROM {table} WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                    {"ids": collision_ids},
                )
        counts[table] = repoint_n
        counts[f"{table}__collision_dropped"] = collision_n
    return counts


def main(argv: list[str] | None = None) -> int:
    """Compute (dry-run) or apply (--apply) the legacy->modern player merge."""
    parser = argparse.ArgumentParser(description="Merge duplicate player identities (legacy->modern)")
    parser.add_argument("--database-url", default=None, help="Local SQLite URL")
    parser.add_argument("--apply", action="store_true", help="Apply the merge (default is dry-run)")
    parser.add_argument("--limit", type=int, default=None, help="Process only the first N clean pairs")
    parser.add_argument("--output-dir", default="data/audit")
    args = parser.parse_args(argv)

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("database URL is required via --database-url or DATABASE_URL")

    engine = create_engine(database_url)
    with engine.connect() as conn:
        report = audit_player_duplicates(conn)
    pairs = report["clean_pairs"]
    if args.limit:
        pairs = pairs[: args.limit]
    print(f"clean_pairs total={report['clean_pair_count']}, processing={len(pairs)}, apply={args.apply}")

    totals: dict[str, int] = {}
    legacy_ids: list[int] = []
    with engine.begin() as conn:
        for pair in pairs:
            legacy_id = pair["legacy_id"]
            modern_id = pair["modern_id"]
            legacy_ids.append(legacy_id)
            counts = _merge_one_pair(conn, legacy_id, modern_id, apply=args.apply)
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
        if args.apply and legacy_ids:
            deleted = conn.execute(
                text("DELETE FROM player_basic WHERE player_id IN :ids").bindparams(bindparam("ids", expanding=True)),
                {"ids": legacy_ids},
            )
            totals["player_basic_deleted"] = deleted.rowcount

    print(json.dumps(totals, ensure_ascii=False, indent=2))
    if not args.apply:
        print("[DRY-RUN] no changes written. Re-run with --apply to apply.")
    else:
        print(f"[APPLY] merged {len(pairs)} pairs; legacy stubs deleted={totals.get('player_basic_deleted', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
