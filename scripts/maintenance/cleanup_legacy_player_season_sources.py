"""Remove selected legacy player-season source rows after creating a backup.

The command is scoped to regular-season rows for 2021 and 2026 by default and
is dry-run unless ``--apply`` is supplied.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import MetaData, case, delete, exists, func, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.schema import Table

from src.db.engine import create_engine_for_url

LEGACY_SOURCES = ("PROFILE", "AGGREGATED", "ROLLUP")
SOURCE_PRIORITY = (
    "CRAWLER",
    "FINAL_VERIFICATION",
    "MANUAL_RECALC",
    "AGGREGATED",
    "RECALC",
    "PROFILE",
    "FALLBACK_BACKFILL",
    "ROLLUP",
)
TARGET_TABLES = ("player_season_batting", "player_season_pitching")
DEFAULT_YEARS = (2021, 2026)
DEFAULT_BACKUP_DIR = Path("data/archive")


def _default_backup_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return DEFAULT_BACKUP_DIR / f"legacy_player_season_sources_{stamp}.json"


def _source_column(table: Table) -> Any:
    return table.c.source if "source" in table.c else table.c.data_source


def _target_filter(table: Table, years: tuple[int, ...]) -> Any:
    source_column = _source_column(table)
    return (
        table.c.season.in_(years),
        table.c.league == "REGULAR",
        source_column.in_(LEGACY_SOURCES),
    )


def _cleanup_filter(table: Table, years: tuple[int, ...]) -> tuple[Any, ...]:
    source_column = _source_column(table)
    higher = table.alias("higher_source")
    higher_source = _source_column(higher)
    key_columns = ["player_id", "season", "league"]
    level_column = "level" if "level" in table.c else "league_level" if "league_level" in table.c else None
    if level_column:
        key_columns.append(level_column)
    key_columns.append("team_code")
    key_match = [getattr(higher.c, key) == getattr(table.c, key) for key in key_columns]
    higher_source_exists = exists(
        select(1)
        .select_from(higher)
        .where(
            *key_match,
            _source_rank(higher_source) < _source_rank(source_column),
        ),
    )
    return (*_target_filter(table, years), higher_source_exists)


def _source_rank(source_column: Any) -> Any:
    return case(
        *((source_column == source, rank) for rank, source in enumerate(SOURCE_PRIORITY)),
        else_=len(SOURCE_PRIORITY),
    )


def _logical_key_columns(table: Table) -> list[str]:
    columns = ["player_id", "season", "league"]
    level_column = "level" if "level" in table.c else "league_level" if "league_level" in table.c else None
    if level_column:
        columns.append(level_column)
    columns.append("team_code")
    return columns


def _reflect_tables(engine: Engine) -> dict[str, Table]:
    metadata = MetaData()
    return {name: Table(name, metadata, autoload_with=engine) for name in TARGET_TABLES}


def _snapshot_rows(
    connection: Any, tables: dict[str, Table], years: tuple[int, ...]
) -> dict[str, list[dict[str, Any]]]:
    snapshot: dict[str, list[dict[str, Any]]] = {}
    for name, table in tables.items():
        rows = connection.execute(select(table).where(*_cleanup_filter(table, years))).mappings().all()
        snapshot[name] = [dict(row) for row in rows]
    return snapshot


def _counts(connection: Any, tables: dict[str, Table], years: tuple[int, ...]) -> dict[str, int]:
    return {
        name: int(
            connection.execute(select(func.count()).select_from(table).where(*_cleanup_filter(table, years))).scalar()
            or 0
        )
        for name, table in tables.items()
    }


def run_cleanup(
    *,
    database_url: str,
    years: tuple[int, ...] = DEFAULT_YEARS,
    apply: bool = False,
    backup_out: Path | None = None,
) -> dict[str, Any]:
    """Back up and optionally remove selected legacy rows."""
    engine = create_engine_for_url(database_url)
    tables = _reflect_tables(engine)
    backup_path = backup_out or _default_backup_path()
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    with engine.connect() as connection:
        snapshot = _snapshot_rows(connection, tables, years)
        before = _counts(connection, tables, years)

    backup_payload = {
        "database": engine.url.get_backend_name(),
        "years": list(years),
        "league": "REGULAR",
        "sources": list(LEGACY_SOURCES),
        "tables": snapshot,
    }
    backup_path.write_text(
        json.dumps(backup_payload, ensure_ascii=False, default=str, indent=2) + "\n", encoding="utf-8"
    )

    deleted = dict.fromkeys(TARGET_TABLES, 0)
    if apply:
        with engine.begin() as connection:
            for name, table in tables.items():
                result = connection.execute(delete(table).where(*_cleanup_filter(table, years)))
                deleted[name] = int(result.rowcount or 0)

    with engine.connect() as connection:
        after = _counts(connection, tables, years)

    return {
        "database": engine.url.get_backend_name(),
        "years": list(years),
        "sources": list(LEGACY_SOURCES),
        "backup_path": str(backup_path),
        "apply": apply,
        "before": before,
        "deleted": deleted,
        "after": after,
    }


def restore_backup(*, database_url: str, backup_path: Path) -> dict[str, Any]:
    """Restore rows from a cleanup backup without overwriting existing IDs."""
    payload = json.loads(backup_path.read_text(encoding="utf-8"))
    engine = create_engine_for_url(database_url)
    tables = _reflect_tables(engine)
    inserted = dict.fromkeys(TARGET_TABLES, 0)
    with engine.begin() as connection:
        for name, table in tables.items():
            rows = payload.get("tables", {}).get(name, [])
            ids = [row.get("id") for row in rows if row.get("id") is not None]
            existing = {row[0] for row in connection.execute(select(table.c.id).where(table.c.id.in_(ids))).all()}
            key_columns = _logical_key_columns(table)
            existing_keys = {
                tuple(row) for row in connection.execute(select(*(table.c[key] for key in key_columns))).all()
            }
            values = [_restore_values(table, row) for row in rows]
            for row, value in zip(rows, values, strict=True):
                row_id = row.get("id")
                logical_key = tuple(row.get(key) for key in key_columns)
                if not value or row_id is None or (logical_key in existing_keys and row_id not in existing):
                    continue
                if row_id in existing:
                    connection.execute(update(table).where(table.c.id == row_id).values(**value))
                else:
                    connection.execute(table.insert().values(**value))
                inserted[name] += 1
                existing_keys.add(logical_key)
    return {"database": engine.url.get_backend_name(), "backup_path": str(backup_path), "inserted": inserted}


def _restore_values(table: Table, row: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for key, value in row.items():
        if key not in table.c:
            continue
        restored_value = value
        if isinstance(value, str):
            try:
                python_type = table.c[key].type.python_type
            except (AttributeError, NotImplementedError):
                python_type = None
            if python_type is datetime:
                restored_value = datetime.fromisoformat(value)
            elif python_type is date:
                restored_value = date.fromisoformat(value[:10])
        values[key] = restored_value
    return values


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Commit deletion. Default is backup plus dry-run.")
    parser.add_argument("--year", action="append", type=int, help="Regular-season year; repeatable.")
    parser.add_argument("--database-url", help="Database URL; defaults to DATABASE_URL.")
    parser.add_argument("--backup-out", type=Path, help="Backup JSON path.")
    parser.add_argument("--restore-backup", type=Path, help="Restore a prior cleanup backup instead of deleting.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the backup and optional cleanup."""
    load_dotenv()
    args = _parser().parse_args(argv)
    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")
    years = tuple(args.year or DEFAULT_YEARS)
    if not years or any(year < 1900 for year in years):
        raise SystemExit("At least one valid season year is required")
    try:
        if args.restore_backup:
            report = restore_backup(database_url=database_url, backup_path=args.restore_backup)
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0
        report = run_cleanup(
            database_url=database_url,
            years=years,
            apply=args.apply,
            backup_out=args.backup_out,
        )
    except (OSError, SQLAlchemyError, TypeError, ValueError) as exc:
        message = f"cleanup failed: {exc}"
        raise SystemExit(message) from exc
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
