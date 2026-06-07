#!/usr/bin/env python3
"""Propose repairs for duplicate batting rows proven to be same-player splits.

Some duplicate groups are not same-name identity conflicts. Modern relay
events may show that every plate appearance for the duplicated player name in
that game belongs to the current ``player_id``. Those groups should be merged,
not remapped to another same-name candidate.

This script is read-only. It writes merge proposals and blocked rows.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

DEFAULT_DB_PATH = Path("data/kbo_dev.db")
DEFAULT_OUTPUT_DIR = Path("data/event_backed_split_repairs")

ADDITIVE_BATTING_COLUMNS = (
    "plate_appearances",
    "at_bats",
    "runs",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "rbi",
    "walks",
    "intentional_walks",
    "hbp",
    "strikeouts",
    "stolen_bases",
    "caught_stealing",
    "sacrifice_hits",
    "sacrifice_flies",
    "gdp",
)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})
            count += 1
    return count


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _norm_int(value: Any) -> int:
    parsed = _safe_int(value)
    return parsed if parsed is not None else 0


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def _duplicate_groups(
    conn: sqlite3.Connection,
    *,
    player_name: str | None = None,
    team_code: str | None = None,
) -> list[sqlite3.Row]:
    filters = ["player_id IS NOT NULL"]
    params: list[Any] = []
    if player_name:
        filters.append("player_name = ?")
        params.append(player_name)
    if team_code:
        filters.append("team_code = ?")
        params.append(team_code)
    where_sql = " AND ".join(filters)
    return conn.execute(
        f"""
        SELECT game_id, player_id, team_side, team_code, player_name, COUNT(*) AS row_count
        FROM game_batting_stats
        WHERE {where_sql}
        GROUP BY game_id, player_id, team_side, team_code, player_name
        HAVING COUNT(*) > 1
        ORDER BY game_id, player_id
        """,
        tuple(params),
    ).fetchall()


def _group_rows(conn: sqlite3.Connection, group: sqlite3.Row) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM game_batting_stats
        WHERE game_id = ?
          AND player_id = ?
          AND team_side = ?
          AND team_code = ?
          AND player_name = ?
        ORDER BY appearance_seq, id
        """,
        (
            group["game_id"],
            group["player_id"],
            group["team_side"],
            group["team_code"],
            group["player_name"],
        ),
    ).fetchall()
    return [dict(row) for row in rows]


def _event_rows(conn: sqlite3.Connection, *, game_id: str, team_side: str, player_name: str) -> list[sqlite3.Row]:
    inning_half = "BOTTOM" if str(team_side).lower() == "home" else "TOP"
    return conn.execute(
        """
        SELECT batter_id, result_code, event_type
        FROM game_events
        WHERE game_id = ?
          AND batter_name = ?
          AND UPPER(COALESCE(inning_half, '')) = ?
        ORDER BY event_seq
        """,
        (game_id, player_name, inning_half),
    ).fetchall()


def _computed_pa(row: dict[str, Any]) -> int:
    return _norm_int(row.get("plate_appearances")) or _norm_int(row.get("at_bats")) + _norm_int(
        row.get("walks")
    ) + _norm_int(row.get("hbp")) + _norm_int(row.get("sacrifice_hits")) + _norm_int(row.get("sacrifice_flies"))


def _merge_values(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {column: sum(_norm_int(row.get(column)) for row in rows) for column in ADDITIVE_BATTING_COLUMNS}


def propose_event_backed_split_repairs(
    *,
    db_path: Path,
    output_dir: Path,
    player_name: str | None = None,
    team_code: str | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")

    proposed_rows: list[dict[str, Any]] = []
    blocked_rows: list[dict[str, Any]] = []
    try:
        if not {"game_batting_stats", "game_events"}.issubset(
            {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        ):
            raise RuntimeError("game_batting_stats and game_events tables are required")

        for group in _duplicate_groups(conn, player_name=player_name, team_code=team_code):
            rows = _group_rows(conn, group)
            events = _event_rows(
                conn,
                game_id=str(group["game_id"]),
                team_side=str(group["team_side"]),
                player_name=str(group["player_name"]),
            )
            event_ids = sorted({int(row["batter_id"]) for row in events if row["batter_id"] is not None})
            base = {
                "game_id": group["game_id"],
                "team_side": group["team_side"],
                "team_code": group["team_code"],
                "player_name": group["player_name"],
                "player_id": group["player_id"],
                "row_count": len(rows),
                "row_ids": ",".join(str(row["id"]) for row in rows),
                "event_batter_ids": ",".join(str(event_id) for event_id in event_ids),
                "event_rows": len(events),
                "computed_pa": sum(_computed_pa(row) for row in rows),
            }
            if len(event_ids) != 1:
                blocked_rows.append(
                    {
                        **base,
                        "reason": "missing_or_ambiguous_event_batter_id",
                        "keeper_id": "",
                        "delete_ids": "",
                    }
                )
                continue
            if event_ids[0] != int(group["player_id"]):
                blocked_rows.append(
                    {
                        **base,
                        "reason": "event_batter_id_differs_from_current_player_id",
                        "keeper_id": "",
                        "delete_ids": "",
                    }
                )
                continue
            if len(events) and base["computed_pa"] != len(events):
                blocked_rows.append(
                    {
                        **base,
                        "reason": "plate_appearance_count_mismatch",
                        "keeper_id": "",
                        "delete_ids": "",
                    }
                )
                continue

            keeper = rows[0]
            delete_ids = [int(row["id"]) for row in rows[1:]]
            proposed_rows.append(
                {
                    **base,
                    "reason": "single_event_batter_id_matches_current_player_id",
                    "keeper_id": keeper["id"],
                    "delete_ids": ",".join(str(row_id) for row_id in delete_ids),
                    **{f"merged_{column}": value for column, value in _merge_values(rows).items()},
                }
            )
    finally:
        conn.close()

    proposed_csv = output_dir / f"event_backed_split_merge_proposals_{stamp}.csv"
    blocked_csv = output_dir / f"event_backed_split_blocked_{stamp}.csv"
    base_fields = [
        "game_id",
        "team_side",
        "team_code",
        "player_name",
        "player_id",
        "row_count",
        "row_ids",
        "event_batter_ids",
        "event_rows",
        "computed_pa",
        "reason",
        "keeper_id",
        "delete_ids",
    ]
    proposed_fields = [*base_fields, *(f"merged_{column}" for column in ADDITIVE_BATTING_COLUMNS)]
    _write_csv(proposed_csv, proposed_rows, proposed_fields)
    _write_csv(blocked_csv, blocked_rows, base_fields)

    return {
        "proposed_groups": len(proposed_rows),
        "blocked_groups": len(blocked_rows),
        "proposed_csv": str(proposed_csv),
        "blocked_csv": str(blocked_csv),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Propose event-backed same-player split batting repairs.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="CSV output directory.")
    parser.add_argument("--player-name", default=None, help="Optional exact player name filter.")
    parser.add_argument("--team-code", default=None, help="Optional exact team code filter.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = propose_event_backed_split_repairs(
        db_path=Path(args.db_path),
        output_dir=Path(args.output_dir),
        player_name=args.player_name,
        team_code=args.team_code,
    )
    print(f"[REPORT] proposed_groups={result['proposed_groups']} blocked_groups={result['blocked_groups']}")
    print(f"[REPORT] proposed_csv={result['proposed_csv']}")
    print(f"[REPORT] blocked_csv={result['blocked_csv']}")


if __name__ == "__main__":
    main()
