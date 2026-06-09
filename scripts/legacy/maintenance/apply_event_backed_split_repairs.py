#!/usr/bin/env python3
"""Apply event-backed same-player split batting merge proposals."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import csv
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.legacy.maintenance.propose_event_backed_split_repairs import ADDITIVE_BATTING_COLUMNS  # noqa: E402

DEFAULT_DB_PATH = Path("data/kbo_dev.db")
DEFAULT_OUTPUT_DIR = Path("data/event_backed_split_repairs")


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _load_proposals(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})")}


def _backup_sqlite_database(db_path: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = output_dir / f"{db_path.name}.backup_before_event_split_merge_{stamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _validate_current_rows(conn: sqlite3.Connection, proposal: dict[str, str]) -> tuple[int, list[int]]:
    keeper_id = _safe_int(proposal.get("keeper_id"))
    delete_ids = [_safe_int(part) for part in str(proposal.get("delete_ids") or "").split(",") if part]
    delete_ids = [row_id for row_id in delete_ids if row_id is not None]
    if keeper_id is None or not delete_ids:
        raise RuntimeError(f"Invalid proposal ids for game={proposal.get('game_id')}")

    all_ids = [keeper_id, *delete_ids]
    placeholders = ",".join("?" for _ in all_ids)
    rows = conn.execute(
        f"""
        SELECT id, game_id, player_id, team_side, team_code, player_name
        FROM game_batting_stats
        WHERE id IN ({placeholders})
        ORDER BY id
        """,
        tuple(all_ids),
    ).fetchall()
    if len(rows) != len(all_ids):
        raise RuntimeError(f"Proposal rows changed or disappeared for ids={all_ids}")

    expected = (
        proposal.get("game_id"),
        _safe_int(proposal.get("player_id")),
        proposal.get("team_side"),
        proposal.get("team_code"),
        proposal.get("player_name"),
    )
    for row in rows:
        current = (
            row["game_id"],
            _safe_int(row["player_id"]),
            row["team_side"],
            row["team_code"],
            row["player_name"],
        )
        if current != expected:
            raise RuntimeError(f"Proposal row identity mismatch for id={row['id']}: {current} != {expected}")
    return keeper_id, delete_ids


def apply_event_backed_split_repairs(
    *,
    db_path: Path,
    proposals_csv: Path,
    output_dir: Path,
    apply: bool = False,
    backup: bool = True,
) -> dict[str, Any]:
    proposals = _load_proposals(proposals_csv)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    backup_path = _backup_sqlite_database(db_path, output_dir) if apply and backup else None
    merged_groups = 0
    deleted_rows = 0
    try:
        columns = _columns(conn, "game_batting_stats")
        update_columns = [column for column in ADDITIVE_BATTING_COLUMNS if column in columns]
        if not update_columns:
            raise RuntimeError("game_batting_stats has none of the expected additive columns")

        for proposal in proposals:
            keeper_id, delete_ids = _validate_current_rows(conn, proposal)
            assignments = {column: _safe_int(proposal.get(f"merged_{column}")) or 0 for column in update_columns}
            if "updated_at" in columns:
                assignments["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if apply:
                set_sql = ", ".join(f"{column} = ?" for column in assignments)
                conn.execute(
                    f"UPDATE game_batting_stats SET {set_sql} WHERE id = ?",
                    (*assignments.values(), keeper_id),
                )
                placeholders = ",".join("?" for _ in delete_ids)
                result = conn.execute(
                    f"DELETE FROM game_batting_stats WHERE id IN ({placeholders})",
                    tuple(delete_ids),
                )
                deleted_rows += int(result.rowcount or 0)
            else:
                deleted_rows += len(delete_ids)
            merged_groups += 1

        if apply:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "dry_run": not apply,
        "merged_groups": merged_groups,
        "deleted_rows": deleted_rows,
        "backup_path": str(backup_path) if backup_path else "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply event-backed same-player batting split merge proposals.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument(
        "--proposals-csv", required=True, help="Proposal CSV from propose_event_backed_split_repairs.py."
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Backup/report output directory.")
    parser.add_argument("--apply", action="store_true", help="Apply the merge proposals. Defaults to dry-run.")
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before --apply.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = apply_event_backed_split_repairs(
        db_path=Path(args.db_path),
        proposals_csv=Path(args.proposals_csv),
        output_dir=Path(args.output_dir),
        apply=bool(args.apply),
        backup=not args.no_backup,
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(f"[{mode}] merged_groups={result['merged_groups']} deleted_rows={result['deleted_rows']}")
    if result["backup_path"]:
        logger.info(f"[BACKUP] {result['backup_path']}")


if __name__ == "__main__":
    main()
