"""Non-destructive per-game_id merge of richer detail rows from a source SQLite DB.

For each detail table (game_id-scoped), compare row counts per game_id between the
main DB and an attached source DB. For any game_id where the source has strictly
more rows, replace the main rows for that game_id with the source rows. game_ids
where main has >= source rows are left untouched. This produces a per-game_id
superset without deleting data that only exists in main.

Usage:
    python3 scripts/maintenance/merge_richer_detail.py --source <path.db> [--apply]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time

DETAIL_TABLES = (
    "game_lineups",
    "game_batting_stats",
    "game_pitching_stats",
    "game_events",
    "game_play_by_play",
    "player_game_batting",
    "player_game_pitching",
    "game_inning_scores",
    "game_summary",
    "game_highlights",
)


def _table_exists(con: sqlite3.Connection, name: str, schema: str = "main") -> bool:
    row = con.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _columns(con: sqlite3.Connection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return [r[1] for r in rows]


def _richer_game_ids(con: sqlite3.Connection, table: str) -> list[str]:
    """Return game_ids where the source (bak) has strictly more rows than main."""
    sql = f"""
        SELECT gid FROM (
            SELECT CAST(game_id AS TEXT) AS gid, COUNT(*) AS c
            FROM bak.{table} GROUP BY CAST(game_id AS TEXT)
        ) b
        LEFT JOIN (
            SELECT CAST(game_id AS TEXT) AS gid, COUNT(*) AS c
            FROM main.{table} GROUP BY CAST(game_id AS TEXT)
        ) m USING (gid)
        WHERE b.c > COALESCE(m.c, 0)
    """
    return [str(r[0]) for r in con.execute(sql).fetchall()]


def _chunks(items: list[str], size: int = 500):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def merge_table(con: sqlite3.Connection, table: str, *, apply: bool) -> dict[str, int]:
    if not _table_exists(con, table, "main") or not _table_exists(con, table, "bak"):
        return {"skipped": 1}
    game_ids = _richer_game_ids(con, table)
    if not game_ids:
        return {"game_ids": 0, "rows_inserted": 0}
    cols = [c for c in _columns(con, table) if c != "id"]
    col_list = ", ".join(cols)
    rows_inserted = 0
    for chunk in _chunks(game_ids):
        placeholders = ",".join("?" * len(chunk))
        if apply:
            con.execute(
                f"DELETE FROM main.{table} WHERE CAST(game_id AS TEXT) IN ({placeholders})",
                chunk,
            )
            cur = con.execute(
                f"INSERT INTO main.{table} ({col_list}) "
                f"SELECT {col_list} FROM bak.{table} WHERE CAST(game_id AS TEXT) IN ({placeholders})",
                chunk,
            )
            rows_inserted += cur.rowcount
        else:
            cnt = con.execute(
                f"SELECT COUNT(*) FROM bak.{table} WHERE CAST(game_id AS TEXT) IN ({placeholders})",
                chunk,
            ).fetchone()[0]
            rows_inserted += cnt
    return {"game_ids": len(game_ids), "rows_inserted": rows_inserted}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge richer per-game_id detail rows from a source SQLite DB")
    parser.add_argument("--db", default="data/kbo_dev.db", help="Main DB path")
    parser.add_argument("--source", required=True, help="Source (richer) DB path to merge from")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args(argv)

    con = sqlite3.connect(args.db, timeout=300)
    con.execute("PRAGMA foreign_keys=OFF")
    con.execute("ATTACH DATABASE ? AS bak", (args.source,))
    total_rows = 0
    total_games = 0
    try:
        if args.apply:
            con.execute("BEGIN")
        for table in DETAIL_TABLES:
            t = time.time()
            result = merge_table(con, table, apply=args.apply)
            if result.get("skipped"):
                print(f"  {table}: SKIP (missing)", flush=True)
                continue
            total_rows += result["rows_inserted"]
            total_games += result["game_ids"]
            verb = "merged" if args.apply else "would merge"
            print(
                f"  {table}: {verb} {result['game_ids']} game_ids, "
                f"{result['rows_inserted']} rows ({time.time() - t:.1f}s)",
                flush=True,
            )
        if args.apply:
            con.commit()
            print("COMMITTED", flush=True)
        else:
            print("DRY-RUN (no changes)", flush=True)
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    print(f"TOTAL: game_ids={total_games} rows={total_rows}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
