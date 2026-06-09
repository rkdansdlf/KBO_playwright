#!/usr/bin/env python3
"""Classify duplicate per-game player rows before any destructive cleanup.

The audit metric groups rows by game/player, but those groups can represent
very different risks: exact duplicate rows, same-name identity collisions, or
conflicting stat payloads. This tool separates those cases and can delete only
provably exact duplicates when explicitly requested.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import csv
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

DEFAULT_DB_PATH = Path("data/kbo_dev.db")
DEFAULT_OUTPUT_DIR = Path("data/duplicate_worklists")
METADATA_COLUMNS = {
    "id",
    "created_at",
    "updated_at",
}
IDENTITY_COLUMNS = {
    "game_id",
    "team_side",
    "team_code",
    "player_id",
    "player_name",
    "batting_order",
    "appearance_seq",
    "position",
    "standard_position",
    "uniform_no",
    "canonical_team_code",
    "franchise_id",
    "is_starter",
    "is_starting",
    "notes",
}


@dataclass(frozen=True)
class DuplicateConfig:
    table_name: str
    group_columns: tuple[str, ...]
    preferred_season_tables: tuple[str, ...]


DUPLICATE_CONFIGS = (
    DuplicateConfig(
        "game_batting_stats", ("game_id", "player_id"), ("player_season_batting", "player_season_pitching")
    ),
    DuplicateConfig("game_pitching_stats", ("game_id", "player_id"), ("player_season_pitching",)),
    DuplicateConfig(
        "game_lineups", ("game_id", "player_id", "team_code"), ("player_season_batting", "player_season_pitching")
    ),
)


def _backup_sqlite_database(db_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = output_dir / f"{db_path.name}.backup_before_exact_duplicate_delete_{stamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        is not None
    )


def _columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    return [str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")]


def _duplicate_groups(conn: sqlite3.Connection, config: DuplicateConfig) -> list[sqlite3.Row]:
    group_expr = ", ".join(config.group_columns)
    return conn.execute(
        f"""
        SELECT {group_expr}, COUNT(*) AS row_count
        FROM {config.table_name}
        WHERE player_id IS NOT NULL
        GROUP BY {group_expr}
        HAVING COUNT(*) > 1
        ORDER BY game_id, player_id
        """
    ).fetchall()


def _group_rows(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    group_columns: tuple[str, ...],
    group: sqlite3.Row,
) -> list[dict[str, Any]]:
    where_sql = " AND ".join(f"{column} = ?" for column in group_columns)
    params = tuple(group[column] for column in group_columns)
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT * FROM {table_name} WHERE {where_sql} ORDER BY id",
            params,
        )
    ]


def _season_from_game_id(game_id: str) -> int | None:
    if len(game_id) >= 4 and game_id[:4].isdigit():
        return int(game_id[:4])
    return None


def _candidate_ids(
    conn: sqlite3.Connection,
    *,
    season_tables: tuple[str, ...],
    season: int,
    team_code: str,
    player_name: str,
) -> tuple[int, ...]:
    candidates: set[int] = set()
    for season_table in season_tables:
        if not _table_exists(conn, season_table):
            continue
        rows = conn.execute(
            f"""
            SELECT DISTINCT ps.player_id
            FROM {season_table} ps
            JOIN player_basic pb ON pb.player_id = ps.player_id
            WHERE ps.season = ?
              AND ps.team_code = ?
              AND pb.name = ?
            """,
            (season, team_code, player_name),
        ).fetchall()
        candidates.update(int(row["player_id"]) for row in rows if row["player_id"] is not None)
    return tuple(sorted(candidates))


def _exact_duplicate(rows: list[dict[str, Any]], columns: list[str]) -> bool:
    compare_columns = [column for column in columns if column not in METADATA_COLUMNS]
    first = {column: rows[0].get(column) for column in compare_columns}
    return all({column: row.get(column) for column in compare_columns} == first for row in rows[1:])


def _stat_payload_differs(rows: list[dict[str, Any]], columns: list[str]) -> bool:
    stat_columns = [
        column
        for column in columns
        if column not in METADATA_COLUMNS and column not in IDENTITY_COLUMNS and column != "extra_stats"
    ]
    first = {column: rows[0].get(column) for column in stat_columns}
    return any({column: row.get(column) for column in stat_columns} != first for row in rows[1:])


def _classify_group(
    conn: sqlite3.Connection,
    *,
    config: DuplicateConfig,
    rows: list[dict[str, Any]],
    columns: list[str],
) -> dict[str, Any]:
    first = rows[0]
    season = _season_from_game_id(str(first.get("game_id") or ""))
    names = sorted({str(row.get("player_name") or "") for row in rows})
    teams = sorted({str(row.get("team_code") or "") for row in rows})
    positions = sorted({str(row.get("standard_position") or row.get("position") or "") for row in rows})
    candidate_ids: tuple[int, ...] = ()
    if season is not None and len(names) == 1 and len(teams) == 1:
        candidate_ids = _candidate_ids(
            conn,
            season_tables=config.preferred_season_tables,
            season=season,
            team_code=teams[0],
            player_name=names[0],
        )

    if _exact_duplicate(rows, columns):
        classification = "exact_duplicate"
    elif len(candidate_ids) > 1:
        classification = "identity_conflict"
    elif _stat_payload_differs(rows, columns):
        classification = "stat_conflict"
    else:
        classification = "metadata_or_lineup_conflict"

    keeper_id = min(int(row["id"]) for row in rows)
    delete_ids = (
        [int(row["id"]) for row in rows if int(row["id"]) != keeper_id] if classification == "exact_duplicate" else []
    )
    group_values = {column: first.get(column) for column in config.group_columns}
    return {
        "table_name": config.table_name,
        **group_values,
        "row_count": len(rows),
        "row_ids": ",".join(str(row["id"]) for row in rows),
        "classification": classification,
        "candidate_ids": ",".join(str(candidate_id) for candidate_id in candidate_ids),
        "player_names": ",".join(names),
        "team_codes": ",".join(teams),
        "positions": ",".join(positions),
        "keeper_id": keeper_id,
        "delete_ids": ",".join(str(row_id) for row_id in delete_ids),
    }


def classify_game_stat_duplicates(
    *,
    db_path: Path,
    output_dir: Path,
    apply_exact: bool,
    backup: bool = True,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = _backup_sqlite_database(db_path, output_dir) if apply_exact and backup else None
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    try:
        group_rows: list[dict[str, Any]] = []
        detail_rows: list[dict[str, Any]] = []
        deleted_rows = 0
        for config in DUPLICATE_CONFIGS:
            if not _table_exists(conn, config.table_name):
                continue
            columns = _columns(conn, config.table_name)
            for group in _duplicate_groups(conn, config):
                rows = _group_rows(
                    conn,
                    table_name=config.table_name,
                    group_columns=config.group_columns,
                    group=group,
                )
                classified = _classify_group(conn, config=config, rows=rows, columns=columns)
                group_rows.append(classified)
                for row in rows:
                    detail_rows.append(
                        {
                            "table_name": config.table_name,
                            "classification": classified["classification"],
                            "candidate_ids": classified["candidate_ids"],
                            **row,
                        }
                    )
                if apply_exact and classified["classification"] == "exact_duplicate" and classified["delete_ids"]:
                    delete_ids = [int(part) for part in str(classified["delete_ids"]).split(",") if part]
                    placeholders = ",".join("?" for _ in delete_ids)
                    result = conn.execute(
                        f"DELETE FROM {config.table_name} WHERE id IN ({placeholders})",
                        tuple(delete_ids),
                    )
                    deleted_rows += int(result.rowcount or 0)

        if apply_exact:
            conn.commit()
        else:
            conn.rollback()

        group_fieldnames = [
            "table_name",
            "game_id",
            "player_id",
            "team_code",
            "row_count",
            "row_ids",
            "classification",
            "candidate_ids",
            "player_names",
            "team_codes",
            "positions",
            "keeper_id",
            "delete_ids",
        ]
        detail_fieldnames = sorted({key for row in detail_rows for key in row}) or ["table_name"]
        groups_csv = output_dir / f"game_stat_duplicate_groups_{stamp}.csv"
        details_csv = output_dir / f"game_stat_duplicate_rows_{stamp}.csv"
        _write_csv(groups_csv, group_rows, group_fieldnames)
        _write_csv(details_csv, detail_rows, detail_fieldnames)

        summary: dict[str, int] = {}
        for row in group_rows:
            key = f"{row['table_name']}:{row['classification']}"
            summary[key] = summary.get(key, 0) + 1
        return {
            "dry_run": not apply_exact,
            "groups": len(group_rows),
            "rows": len(detail_rows),
            "deleted_rows": deleted_rows,
            "summary": summary,
            "groups_csv": str(groups_csv),
            "details_csv": str(details_csv),
            "backup_path": str(backup_path) if backup_path else "",
        }
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify per-game duplicate player rows.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="CSV report directory.")
    parser.add_argument("--apply-exact", action="store_true", help="Delete only exact duplicate rows.")
    parser.add_argument("--no-backup", action="store_true", help="Skip backup before --apply-exact.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = classify_game_stat_duplicates(
        db_path=Path(args.db_path),
        output_dir=Path(args.output_dir),
        apply_exact=bool(args.apply_exact),
        backup=not args.no_backup,
    )
    mode = "APPLY-EXACT" if args.apply_exact else "DRY-RUN"
    logger.info(f"[{mode}] groups={result['groups']} rows={result['rows']} deleted_rows={result['deleted_rows']}")
    for key in sorted(result["summary"]):
        logger.info(f"  {key}={result['summary'][key]}")
    if result["backup_path"]:
        logger.info(f"[BACKUP] {result['backup_path']}")
    logger.info(f"[REPORT] groups={result['groups_csv']}")
    logger.info(f"[REPORT] rows={result['details_csv']}")


if __name__ == "__main__":
    main()
