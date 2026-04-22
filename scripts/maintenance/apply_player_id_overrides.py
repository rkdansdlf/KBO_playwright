#!/usr/bin/env python3
"""Apply curated player_id overrides to local SQLite or OCI.

Group overrides are intentionally conservative: they only update NULL values by
default, or generated local IDs when --include-generated is passed. Row
overrides are exact game_id/appearance_seq patches and may correct non-null
misassignments.
"""
from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL


DEFAULT_GROUP_OVERRIDES = PROJECT_ROOT / "data/player_id_overrides.csv"
DEFAULT_ROW_OVERRIDES = PROJECT_ROOT / "data/player_id_row_overrides.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/player_id_override_apply"
ALLOWED_TABLES = {"game_batting_stats", "game_pitching_stats", "game_lineups"}


@dataclass(frozen=True)
class GroupOverride:
    source_table: str
    year: int
    team_code: str
    player_name: str
    resolved_player_id: int
    reason: str
    evidence_source: str


@dataclass(frozen=True)
class RowOverride:
    source_table: str
    game_id: str
    appearance_seq: int
    team_code: str
    player_name: str
    resolved_player_id: int
    reason: str
    evidence_source: str


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    return Path(db_url.removeprefix("sqlite:///"))


def _backup_sqlite_database(db_url: str, output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(db_url)
    if db_path is None or not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_path = output_dir / f"{db_path.name}.backup_before_player_id_overrides_{stamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    fieldnames = [
        "override_type",
        "source_table",
        "year",
        "game_id",
        "appearance_seq",
        "team_code",
        "player_name",
        "resolved_player_id",
        "matched_rows",
        "updated_rows",
        "reason",
        "evidence_source",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _valid_table(value: str) -> str:
    table_name = str(value or "").strip()
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported source_table: {table_name}")
    return table_name


def load_group_overrides(path: Path) -> list[GroupOverride]:
    if not path.exists():
        return []
    rows: list[GroupOverride] = []
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            table = str(row.get("source_table") or "").strip()
            year = str(row.get("year") or "").strip()
            team_code = str(row.get("team_code") or "").strip()
            player_name = str(row.get("player_name") or "").strip()
            resolved_player_id = str(row.get("resolved_player_id") or "").strip()
            if not table or not year or not team_code or not player_name or not resolved_player_id:
                continue
            rows.append(
                GroupOverride(
                    source_table=_valid_table(table),
                    year=int(year),
                    team_code=team_code,
                    player_name=player_name,
                    resolved_player_id=int(resolved_player_id),
                    reason=str(row.get("reason") or "").strip(),
                    evidence_source=str(row.get("evidence_source") or "").strip(),
                )
            )
    return rows


def load_row_overrides(path: Path) -> list[RowOverride]:
    if not path.exists():
        return []
    rows: list[RowOverride] = []
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            table = str(row.get("source_table") or "").strip()
            game_id = str(row.get("game_id") or "").strip()
            appearance_seq = str(row.get("appearance_seq") or "").strip()
            team_code = str(row.get("team_code") or "").strip()
            player_name = str(row.get("player_name") or "").strip()
            resolved_player_id = str(row.get("resolved_player_id") or "").strip()
            if not table or not game_id or not appearance_seq or not team_code or not player_name or not resolved_player_id:
                continue
            rows.append(
                RowOverride(
                    source_table=_valid_table(table),
                    game_id=game_id,
                    appearance_seq=int(appearance_seq),
                    team_code=team_code,
                    player_name=player_name,
                    resolved_player_id=int(resolved_player_id),
                    reason=str(row.get("reason") or "").strip(),
                    evidence_source=str(row.get("evidence_source") or "").strip(),
                )
            )
    return rows


def _group_where_sql(table_name: str, include_generated: bool) -> str:
    player_filter = "player_id IS NULL"
    if include_generated:
        player_filter = "(player_id IS NULL OR player_id >= 900000)"
    conflict_guard = ""
    if table_name in {"game_batting_stats", "game_pitching_stats"}:
        conflict_guard = f"""
          AND NOT EXISTS (
              SELECT 1
              FROM {table_name} existing
              WHERE existing.game_id = {table_name}.game_id
                AND existing.appearance_seq = {table_name}.appearance_seq
                AND existing.player_id = :player_id
                AND existing.id != {table_name}.id
          )
        """
    return f"""
        WHERE {player_filter}
          AND substr(game_id, 1, 4) = :year
          AND COALESCE(team_code, '') = :team_code
          AND player_name = :player_name
          {conflict_guard}
    """


def _row_where_sql(table_name: str) -> str:
    conflict_guard = ""
    if table_name in {"game_batting_stats", "game_pitching_stats"}:
        conflict_guard = f"""
          AND NOT EXISTS (
              SELECT 1
              FROM {table_name} existing
              WHERE existing.game_id = {table_name}.game_id
                AND existing.appearance_seq = {table_name}.appearance_seq
                AND existing.player_id = :player_id
                AND existing.id != {table_name}.id
          )
        """
    return f"""
        WHERE game_id = :game_id
          AND appearance_seq = :appearance_seq
          AND COALESCE(team_code, '') = :team_code
          AND player_name = :player_name
          AND (player_id IS NULL OR player_id != :player_id)
          {conflict_guard}
    """


def _apply_group(conn, override: GroupOverride, *, include_generated: bool, apply: bool) -> dict[str, Any]:
    table_name = override.source_table
    where_sql = _group_where_sql(table_name, include_generated)
    params = {
        "player_id": override.resolved_player_id,
        "year": str(override.year),
        "team_code": override.team_code,
        "player_name": override.player_name,
    }
    matched = int(conn.execute(text(f"SELECT COUNT(*) FROM {table_name} {where_sql}"), params).scalar() or 0)
    updated = 0
    if apply and matched:
        result = conn.execute(text(f"UPDATE {table_name} SET player_id = :player_id {where_sql}"), params)
        updated = int(result.rowcount or 0)
    return {
        "override_type": "group",
        "source_table": table_name,
        "year": override.year,
        "team_code": override.team_code,
        "player_name": override.player_name,
        "resolved_player_id": override.resolved_player_id,
        "matched_rows": matched,
        "updated_rows": updated,
        "reason": override.reason,
        "evidence_source": override.evidence_source,
    }


def _apply_row(conn, override: RowOverride, *, apply: bool) -> dict[str, Any]:
    table_name = override.source_table
    where_sql = _row_where_sql(table_name)
    params = {
        "player_id": override.resolved_player_id,
        "game_id": override.game_id,
        "appearance_seq": override.appearance_seq,
        "team_code": override.team_code,
        "player_name": override.player_name,
    }
    matched = int(conn.execute(text(f"SELECT COUNT(*) FROM {table_name} {where_sql}"), params).scalar() or 0)
    updated = 0
    if apply and matched:
        result = conn.execute(text(f"UPDATE {table_name} SET player_id = :player_id {where_sql}"), params)
        updated = int(result.rowcount or 0)
    return {
        "override_type": "row",
        "source_table": table_name,
        "game_id": override.game_id,
        "appearance_seq": override.appearance_seq,
        "team_code": override.team_code,
        "player_name": override.player_name,
        "resolved_player_id": override.resolved_player_id,
        "matched_rows": matched,
        "updated_rows": updated,
        "reason": override.reason,
        "evidence_source": override.evidence_source,
    }


def apply_overrides(
    *,
    db_url: str,
    group_overrides_csv: Path,
    row_overrides_csv: Path,
    output_dir: Path,
    years: set[int] | None,
    tables: set[str] | None,
    include_generated: bool,
    apply: bool,
    backup: bool,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = _backup_sqlite_database(db_url, output_dir) if apply and backup else None
    group_overrides = load_group_overrides(group_overrides_csv)
    row_overrides = load_row_overrides(row_overrides_csv)
    if years is not None:
        group_overrides = [row for row in group_overrides if row.year in years]
        row_overrides = [row for row in row_overrides if int(row.game_id[:4]) in years]
    if tables is not None:
        group_overrides = [row for row in group_overrides if row.source_table in tables]
        row_overrides = [row for row in row_overrides if row.source_table in tables]

    engine = create_engine(db_url)
    report_rows: list[dict[str, Any]] = []
    with engine.begin() as conn:
        for override in group_overrides:
            report_rows.append(_apply_group(conn, override, include_generated=include_generated, apply=apply))
        for override in row_overrides:
            report_rows.append(_apply_row(conn, override, apply=apply))
        if not apply:
            conn.rollback()

    report_csv = output_dir / f"player_id_override_apply_{stamp}.csv"
    _write_csv(report_csv, report_rows)
    return {
        "dry_run": not apply,
        "group_overrides": len(group_overrides),
        "row_overrides": len(row_overrides),
        "matched_rows": sum(int(row["matched_rows"]) for row in report_rows),
        "updated_rows": sum(int(row["updated_rows"]) for row in report_rows),
        "report_csv": str(report_csv),
        "backup_path": str(backup_path) if backup_path else "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply curated player_id overrides.")
    parser.add_argument("--oci", action="store_true", help="Apply to OCI_DB_URL instead of local DATABASE_URL.")
    parser.add_argument("--years", default="", help="Comma-separated years to inspect.")
    parser.add_argument("--tables", default="", help="Comma-separated table names to inspect.")
    parser.add_argument("--group-overrides-csv", default=str(DEFAULT_GROUP_OVERRIDES))
    parser.add_argument("--row-overrides-csv", default=str(DEFAULT_ROW_OVERRIDES))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--include-generated", action="store_true", help="Group overrides may update player_id >= 900000.")
    parser.add_argument("--apply", action="store_true", help="Persist updates. Default is dry-run only.")
    parser.add_argument("--no-backup", action="store_true", help="Skip SQLite backup before local --apply.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv()
    db_url = os.getenv("OCI_DB_URL") if args.oci else DATABASE_URL
    if not db_url:
        raise SystemExit("OCI_DB_URL is required with --oci")
    years = {int(part.strip()) for part in args.years.split(",") if part.strip()} or None
    tables = {_valid_table(part.strip()) for part in args.tables.split(",") if part.strip()} or None
    result = apply_overrides(
        db_url=db_url,
        group_overrides_csv=Path(args.group_overrides_csv),
        row_overrides_csv=Path(args.row_overrides_csv),
        output_dir=Path(args.output_dir),
        years=years,
        tables=tables,
        include_generated=bool(args.include_generated),
        apply=bool(args.apply),
        backup=not args.no_backup,
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(
        f"[{mode}] group_overrides={result['group_overrides']} row_overrides={result['row_overrides']} "
        f"matched_rows={result['matched_rows']} updated_rows={result['updated_rows']}"
    )
    if result["backup_path"]:
        print(f"[BACKUP] {result['backup_path']}")
    print(f"[REPORT] {result['report_csv']}")


if __name__ == "__main__":
    main()
