"""Migrate the local SQLite database into the independent PostgreSQL database."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from typing import Any

from sqlalchemy import JSON, Date, DateTime, Engine, Time, create_engine, func, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.schema import Table

from src.db.engine import _ensure_stat_recalc_view
from src.models.base import Base

logger = logging.getLogger(__name__)
DEFAULT_SOURCE_URL = "sqlite:///./data/kbo_dev.db"
DEFAULT_BATCH_SIZE = 1000


@dataclass(frozen=True)
class TableMigrationReport:
    """Report the source, target, and copied row counts for one table."""

    table: str
    source_rows: int
    target_rows: int
    copied_rows: int = 0
    status: str = "pending"
    reason: str | None = None


def _coerce_value(value: object, target_type: object) -> object:
    """Convert common SQLite values to PostgreSQL-compatible Python values."""
    if value is None:
        return None
    if isinstance(target_type, JSON):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return value
    if isinstance(target_type, DateTime) and isinstance(value, str):
        return datetime.fromisoformat(value)
    if isinstance(target_type, Date) and isinstance(value, str):
        return date.fromisoformat(value[:10])
    if isinstance(target_type, Time) and isinstance(value, str):
        return time.fromisoformat(value)
    if target_type.__class__.__name__ == "Boolean" and isinstance(value, (int, str)):
        return str(value).lower() not in {"0", "false", "no", ""}
    return value


def _model_tables() -> list[Table]:
    """Load all ORM models and return tables in dependency order."""
    import src.models  # noqa: F401

    return list(Base.metadata.sorted_tables)


def _count_rows(connection: Any, table: Table) -> int:
    return int(connection.execute(select(func.count()).select_from(table)).scalar_one())


def _table_payload(table: Table, row: Any, target_table: Table) -> dict[str, object]:
    source_values = row._mapping
    return {
        column.name: _coerce_value(source_values[column.name], target_table.c[column.name].type)
        for column in target_table.columns
        if column.name in source_values
    }


def _reset_sequence(connection: Any, table: Table) -> None:
    primary_keys = list(table.primary_key.columns)
    if len(primary_keys) != 1 or primary_keys[0].name != "id":
        return
    sequence = connection.execute(
        text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
        {"table_name": table.name, "column_name": "id"},
    ).scalar_one_or_none()
    if not sequence:
        return
    maximum = connection.execute(select(func.max(table.c.id))).scalar_one_or_none()
    if maximum is None:
        return
    connection.execute(
        text("SELECT setval(CAST(:sequence_name AS regclass), :last_value, true)"),
        {"sequence_name": sequence, "last_value": int(maximum)},
    )


def _quote_identifier(identifier: str) -> str:
    """Quote a PostgreSQL identifier used in a COPY statement."""
    return '"' + identifier.replace('"', '""') + '"'


def _copy_value(value: object, target_type: object) -> str:
    """Convert a Python value to PostgreSQL COPY text."""
    if value is None:
        return r"\N"
    if target_type.__class__.__name__.endswith("ARRAY") and isinstance(value, list):
        items = (str(item).replace("\\", "\\\\").replace('"', '\\"') for item in value)
        return "{" + ",".join(f'"{item}"' for item in items) + "}"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _copy_rows(connection: Any, table: Table, rows: list[dict[str, object]]) -> None:
    """Bulk-copy one converted batch into a PostgreSQL table."""
    if not rows:
        return
    columns = list(rows[0])
    column_types = {
        column: table.c[column].type.dialect_impl(connection.dialect)
        for column in columns
    }
    for column, target_type in column_types.items():
        length = getattr(target_type, "length", None)
        if not length:
            continue
        if any(
            len(_copy_value(row[column], target_type)) > length
            for row in rows
            if row[column] is not None
        ):
            connection.execute(
                text(
                    f"ALTER TABLE {_quote_identifier(table.name)} "
                    f"ALTER COLUMN {_quote_identifier(column)} TYPE TEXT"
                )
            )
            column_types[column] = object()
    statement = (
        f"COPY {_quote_identifier(table.name)} ({', '.join(_quote_identifier(column) for column in columns)}) "
        "FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    )
    payload = io.StringIO()
    writer = csv.writer(payload, lineterminator="\n")
    writer.writerows(
        [
            _copy_value(row[column], column_types[column])
            for column in columns
        ]
        for row in rows
    )
    payload.seek(0)
    cursor = connection.connection.cursor()
    try:
        cursor.copy_expert(statement, payload)
    finally:
        cursor.close()


def _inspect_tables(source_engine: Engine, target_engine: Engine) -> list[TableMigrationReport]:
    source_inspector = inspect(source_engine)
    target_inspector = inspect(target_engine)
    source_names = set(source_inspector.get_table_names())
    reports: list[TableMigrationReport] = []
    with source_engine.connect() as source, target_engine.connect() as target:
        for table in _model_tables():
            if table.name not in source_names:
                continue
            source_rows = _count_rows(source, table)
            target_rows = _count_rows(target, table) if target_inspector.has_table(table.name) else 0
            reports.append(TableMigrationReport(table.name, source_rows, target_rows))
    return reports


def _existing_target_rows(target_engine: Engine, tables: list[Table]) -> list[str]:
    target_inspector = inspect(target_engine)
    existing: list[str] = []
    with target_engine.connect() as target:
        for table in tables:
            if target_inspector.has_table(table.name) and _count_rows(target, table):
                existing.append(table.name)
    return existing


def migrate(
    *, source_url: str, target_url: str, apply: bool, batch_size: int = DEFAULT_BATCH_SIZE
) -> list[TableMigrationReport]:
    """Plan or apply a SQLite-to-PostgreSQL migration."""
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    try:
        if source_engine.dialect.name != "sqlite":
            raise ValueError("source database must be SQLite")
        if target_engine.dialect.name != "postgresql":
            raise ValueError("target database must be PostgreSQL")

        tables = _model_tables()
        reports = _inspect_tables(source_engine, target_engine)
        if not apply:
            return reports
        existing_tables = _existing_target_rows(target_engine, tables)
        if existing_tables:
            details = ", ".join(existing_tables[:10])
            suffix = "..." if len(existing_tables) > 10 else ""
            msg = f"Target PostgreSQL database is not empty; refusing to merge existing tables: {details}{suffix}"
            raise RuntimeError(msg)

        Base.metadata.create_all(bind=target_engine)
        _ensure_stat_recalc_view(target_engine)
        target_tables = {table.name: table for table in tables}
        with source_engine.connect() as source, target_engine.begin() as target:
            for report in reports:
                source_table = target_tables[report.table]
                rows = source.execute(select(source_table))
                batch: list[dict[str, object]] = []
                copied = 0
                for row in rows:
                    batch.append(_table_payload(source_table, row, source_table))
                    if len(batch) >= batch_size:
                        _copy_rows(target, source_table, batch)
                        copied += len(batch)
                        batch.clear()
                if batch:
                    _copy_rows(target, source_table, batch)
                    copied += len(batch)
                _reset_sequence(target, source_table)
                report_index = reports.index(report)
                reports[report_index] = TableMigrationReport(
                    report.table,
                    report.source_rows,
                    report.target_rows,
                    copied_rows=copied,
                    status="copied",
                )
        return reports
    finally:
        source_engine.dispose()
        target_engine.dispose()


def main(argv: list[str] | None = None) -> int:
    """Run a dry-run or apply the SQLite-to-PostgreSQL migration."""
    parser = argparse.ArgumentParser(description="Migrate local SQLite data into PostgreSQL")
    parser.add_argument("--source-url", default=os.getenv("SOURCE_DATABASE_URL", DEFAULT_SOURCE_URL))
    parser.add_argument("--target-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--apply", action="store_true", help="Write rows to PostgreSQL")
    args = parser.parse_args(argv)
    if not args.target_url:
        msg = "DATABASE_URL or --target-url is required"
        raise SystemExit(msg)
    if args.batch_size < 1:
        msg = "--batch-size must be positive"
        raise SystemExit(msg)
    try:
        reports = migrate(
            source_url=args.source_url,
            target_url=args.target_url,
            apply=args.apply,
            batch_size=args.batch_size,
        )
    except (SQLAlchemyError, RuntimeError, ValueError, OSError):
        logger.exception("SQLite to PostgreSQL migration failed")
        return 1
    payload = {"apply": args.apply, "tables": [asdict(report) for report in reports]}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
