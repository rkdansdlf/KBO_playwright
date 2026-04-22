#!/usr/bin/env python3
"""Fill remaining local NULL player_id values from matching OCI rows."""
from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import bindparam, create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL, SessionLocal


DEFAULT_TABLES = ("game_batting_stats", "game_pitching_stats", "game_lineups")
MATCH_COLUMNS = {
    "game_batting_stats": ("game_id", "team_side", "team_code", "player_name", "appearance_seq"),
    "game_pitching_stats": ("game_id", "team_side", "team_code", "player_name", "appearance_seq"),
    "game_lineups": ("game_id", "team_side", "team_code", "player_name", "appearance_seq"),
}


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    return Path(db_url.removeprefix("sqlite:///"))


def _backup_sqlite_database(output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(DATABASE_URL)
    if db_path is None or not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = output_dir / f"{db_path.name}.backup_before_oci_player_id_fill_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_path, backup_path)
    return backup_path


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _local_null_rows(session, table_name: str, years: tuple[int, ...]) -> list[dict[str, Any]]:
    stmt = text(
        f"""
        SELECT id, game_id, team_side, COALESCE(team_code, '') AS team_code, player_name, appearance_seq
        FROM {table_name}
        WHERE player_id IS NULL
          AND substr(game_id, 1, 4) IN :years
        ORDER BY game_id, id
        """
    ).bindparams(bindparam("years", expanding=True))
    rows = session.execute(stmt, {"years": [str(year) for year in years]}).mappings().all()
    return [dict(row) for row in rows]


def _remote_candidate_ids(conn, table_name: str, row: dict[str, Any]) -> list[int]:
    match_columns = MATCH_COLUMNS[table_name]
    where = " AND ".join(
        f"COALESCE(CAST({column} AS TEXT), '') = :{column}"
        if column == "team_code"
        else f"{column} = :{column}"
        for column in match_columns
    )
    params = {
        column: str(row[column] or "") if column == "team_code" else row[column]
        for column in match_columns
    }
    rows = conn.execute(
        text(
            f"""
            SELECT DISTINCT player_id
            FROM {table_name}
            WHERE {where}
              AND player_id IS NOT NULL
            """
        ),
        params,
    ).fetchall()
    return sorted({int(row[0]) for row in rows})


def _match_key(table_name: str, row: dict[str, Any]) -> tuple[Any, ...]:
    key = []
    for column in MATCH_COLUMNS[table_name]:
        if column == "team_code":
            key.append(str(row.get(column) or ""))
        else:
            key.append(row.get(column))
    return tuple(key)


def _batches(values: list[str], size: int = 500) -> list[list[str]]:
    return [values[index:index + size] for index in range(0, len(values), size)]


def _remote_candidate_map(conn, table_name: str, local_rows: list[dict[str, Any]]) -> dict[tuple[Any, ...], list[int]]:
    if not local_rows:
        return {}
    game_ids = sorted({str(row["game_id"]) for row in local_rows})
    candidate_sets: dict[tuple[Any, ...], set[int]] = defaultdict(set)
    stmt = text(
        f"""
        SELECT
            game_id,
            team_side,
            COALESCE(CAST(team_code AS TEXT), '') AS team_code,
            player_name,
            appearance_seq,
            player_id
        FROM {table_name}
        WHERE game_id IN :game_ids
          AND player_id IS NOT NULL
        """
    ).bindparams(bindparam("game_ids", expanding=True))
    for game_id_batch in _batches(game_ids):
        for row in conn.execute(stmt, {"game_ids": game_id_batch}).mappings():
            row_dict = dict(row)
            candidate_sets[_match_key(table_name, row_dict)].add(int(row_dict["player_id"]))
    return {key: sorted(value) for key, value in candidate_sets.items()}


def _is_generated_player_id(player_id: int) -> bool:
    return int(player_id) >= 900000


def _choose_candidate_id(candidate_ids: list[int]) -> int | None:
    unique_ids = sorted(set(candidate_ids))
    if len(unique_ids) == 1:
        return unique_ids[0]
    real_ids = [player_id for player_id in unique_ids if not _is_generated_player_id(player_id)]
    if len(real_ids) == 1:
        return real_ids[0]
    return None


def fill_from_oci(
    *,
    oci_url: str,
    years: tuple[int, ...],
    tables: tuple[str, ...],
    output_dir: Path,
    apply: bool,
    backup: bool,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = _backup_sqlite_database(output_dir) if apply and backup else None
    oci_engine = create_engine(oci_url)
    resolved: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    updated_rows = 0

    try:
        with SessionLocal() as local_session, oci_engine.connect() as remote_conn:
            for table_name in tables:
                local_rows = _local_null_rows(local_session, table_name, years)
                candidate_map = _remote_candidate_map(remote_conn, table_name, local_rows)
                for row in local_rows:
                    candidates = candidate_map.get(_match_key(table_name, row), [])
                    report_row = {
                        "table_name": table_name,
                        "local_id": row["id"],
                        "game_id": row["game_id"],
                        "team_side": row["team_side"],
                        "team_code": row["team_code"],
                        "player_name": row["player_name"],
                        "appearance_seq": row["appearance_seq"],
                        "candidate_ids": ",".join(str(pid) for pid in candidates),
                    }
                    resolved_player_id = _choose_candidate_id(candidates)
                    if resolved_player_id is not None:
                        if apply:
                            result = local_session.execute(
                                text(f"UPDATE {table_name} SET player_id = :player_id WHERE id = :id AND player_id IS NULL"),
                                {"player_id": resolved_player_id, "id": row["id"]},
                            )
                            updated_rows += int(result.rowcount or 0)
                        else:
                            updated_rows += 1
                        resolved.append({**report_row, "resolved_player_id": resolved_player_id})
                    else:
                        unresolved.append({**report_row, "resolved_player_id": ""})
            if apply:
                local_session.commit()
            else:
                local_session.rollback()
    finally:
        oci_engine.dispose()

    fieldnames = [
        "table_name",
        "local_id",
        "game_id",
        "team_side",
        "team_code",
        "player_name",
        "appearance_seq",
        "candidate_ids",
        "resolved_player_id",
    ]
    resolved_csv = output_dir / f"null_player_id_oci_resolved_{stamp}.csv"
    unresolved_csv = output_dir / f"null_player_id_oci_unresolved_{stamp}.csv"
    _write_csv(resolved_csv, resolved, fieldnames)
    _write_csv(unresolved_csv, unresolved, fieldnames)
    return {
        "dry_run": not apply,
        "resolved_rows": len(resolved),
        "unresolved_rows": len(unresolved),
        "updated_rows": updated_rows,
        "resolved_csv": str(resolved_csv),
        "unresolved_csv": str(unresolved_csv),
        "backup_path": str(backup_path) if backup_path else "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fill local NULL player_id values from matching OCI rows.")
    parser.add_argument("--oci-url", default=None, help="OCI/Postgres URL. Defaults to OCI_DB_URL.")
    parser.add_argument("--years", default="2024,2025", help="Comma-separated years.")
    parser.add_argument("--tables", default=",".join(DEFAULT_TABLES), help="Comma-separated table names.")
    parser.add_argument("--output-dir", default="data", help="Report output directory.")
    parser.add_argument("--apply", action="store_true", help="Persist updates. Default is dry-run only.")
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before --apply.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    oci_url = args.oci_url or os.getenv("OCI_DB_URL")
    if not oci_url:
        raise SystemExit("OCI_DB_URL or --oci-url is required")
    years = tuple(int(part.strip()) for part in args.years.split(",") if part.strip())
    tables = tuple(part.strip() for part in args.tables.split(",") if part.strip())
    result = fill_from_oci(
        oci_url=oci_url,
        years=years,
        tables=tables,
        output_dir=Path(args.output_dir),
        apply=bool(args.apply),
        backup=not args.no_backup,
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] resolved_rows={result['resolved_rows']} "
        f"unresolved_rows={result['unresolved_rows']} updated_rows={result['updated_rows']}"
    )
    if result["backup_path"]:
        print(f"[BACKUP] {result['backup_path']}")
    print(f"[REPORT] resolved={result['resolved_csv']}")
    print(f"[REPORT] unresolved={result['unresolved_csv']}")


if __name__ == "__main__":
    main()
