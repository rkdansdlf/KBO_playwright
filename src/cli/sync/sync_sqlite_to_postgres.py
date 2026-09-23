"""One-time SQLite to local PostgreSQL initial-load CLI.

Reads every application table from the SQLite source file and bulk-inserts
it into a fresh PostgreSQL database whose schema was created from the ORM
baseline (``python3 -m src.cli.apply_postgres_migrations``).

Features:
- Dependency-level table ordering via ``src.sync.table_dag.TABLE_REGISTRY``.
- Row-by-row type conversion from SQLite storage to PostgreSQL column types
  (booleans, datetimes, JSON payloads).
- Dry-run by default; use ``--apply`` to persist changes.
- Resume-safe apply: tables whose target count already matches the source
  are skipped, partially loaded tables are reloaded.
- Verification mode compares row counts without writing.
- JSON output for automation pipelines.

Usage:
    python3 -m src.cli.sync_sqlite_to_postgres --dry-run
    python3 -m src.cli.sync_sqlite_to_postgres --apply --level 0 --level 1
    python3 -m src.cli.sync_sqlite_to_postgres --verify --json
"""

from __future__ import annotations

# ruff: noqa: T201
import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from datetime import time as dtime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv
from sqlalchemy import (
    JSON,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    Time,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from src.sync.table_dag import TABLE_REGISTRY, TableMeta

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from sqlalchemy import Table
    from sqlalchemy.engine import Connection, Engine

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sync_sqlite_to_postgres")

DEFAULT_SQLITE_URL = "sqlite:///./data/kbo_dev.db"
DEFAULT_BATCH_SIZE = 5000
SKIP_TABLES = frozenset({"_sync_checkpoints", "schema_migrations", "sqlite_sequence"})
_TRUE_TOKENS = frozenset({"1", "t", "true", "y", "yes"})
_FALSE_TOKENS = frozenset({"0", "f", "false", "n", "no", ""})


@dataclass
class TableSyncResult:
    """Represents the execution result for a single table transfer."""

    table_name: str
    level: int
    action: str  # SKIP, LOAD, RELOAD, DRY_RUN, FAILED
    source_count: int
    target_before: int
    target_after: int | None = None
    elapsed_seconds: float = 0.0
    deduped: int = 0
    message: str | None = None


@dataclass
class SyncReport:
    """Aggregated transfer run report."""

    started_at: str
    completed_at: str
    total_elapsed_seconds: float
    mode: str
    apply: bool
    tables_total: int
    tables_synced: int
    tables_skipped: int
    tables_failed: int
    rows_synced: int = 0
    results: list[TableSyncResult] = field(default_factory=list)


@dataclass
class SyncOptions:
    """Configuration options for the transfer run."""

    batch_size: int = DEFAULT_BATCH_SIZE
    apply_changes: bool = False
    levels: list[int] | None = None
    tables: list[str] | None = None


@dataclass
class TableTransferRequest:
    """Per-table transfer request bundling source, target, and runtime options."""

    source_path: str
    target_engine: Engine
    target_table: Table
    level: int = 0
    batch_size: int = DEFAULT_BATCH_SIZE
    apply: bool = False


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for the transfer CLI."""
    parser = argparse.ArgumentParser(description="Load SQLite data into local PostgreSQL.")
    parser.add_argument("--source-url", default=DEFAULT_SQLITE_URL, help="Source SQLite URL.")
    parser.add_argument("--target-url", default=None, help="Target URL (default: DATABASE_URL).")
    parser.add_argument("--apply", action="store_true", help="Persist changes (default is dry-run).")
    parser.add_argument("--verify", action="store_true", help="Compare row counts without writing.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows per batch.")
    parser.add_argument("--level", type=int, action="append", default=None, help="Restrict to a level.")
    parser.add_argument("--tables", type=str, default=None, help="Comma-separated table allowlist.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    return parser.parse_args(argv)


def _sqlite_path(source_url: str) -> str:
    """Resolve a SQLite URL to a filesystem path."""
    url = make_url(source_url)
    if url.drivername != "sqlite":
        msg = f"Source must be a SQLite URL, got {url.drivername}"
        raise ValueError(msg)
    return url.database or ""


def _convert_bool(value: object) -> bool | None:
    """Convert a SQLite-stored flag to a Python boolean."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in _TRUE_TOKENS:
        return True
    return normalized not in _FALSE_TOKENS


def _convert_datetime(value: object) -> datetime | None:
    """Convert a SQLite-stored timestamp to a naive Python datetime."""
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)  # noqa: DTZ001
    raw = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def _convert_date(value: object) -> date | None:
    """Convert a SQLite-stored day value to a Python date."""
    if value is None or isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _convert_time(value: object) -> dtime | None:
    """Convert a SQLite-stored clock value to a Python time."""
    if value is None or isinstance(value, dtime):
        return value
    try:
        return dtime.fromisoformat(str(value).strip())
    except ValueError:
        return None


def _convert_json(value: object) -> object:
    """Convert a SQLite-stored JSON document to Python objects."""
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


def _parse_int_text(raw: str) -> int | None:
    """Parse stripped text as an integer, tolerating float spellings."""
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return int(float(raw))
    except ValueError:
        return None


def _convert_int(value: object) -> int | None:
    """Convert a SQLite-stored whole number to a Python int."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return None
    return _parse_int_text(raw)


def _convert_float(value: object) -> float | None:
    """Convert a SQLite-stored real number to a Python float."""
    if value is None or isinstance(value, float):
        return value
    if isinstance(value, bool):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _convert_numeric(value: object) -> Decimal | None:
    """Convert a SQLite-stored fixed-point number to a Decimal."""
    if value is None or isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(int(value))
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


_CONVERTER_RULES: tuple[tuple[type, Callable[[object], Any]], ...] = (
    (Boolean, _convert_bool),
    (DateTime, _convert_datetime),
    (Date, _convert_date),
    (Time, _convert_time),
    (JSON, _convert_json),
    (Integer, _convert_int),
    (Numeric, _convert_numeric),
    (Float, _convert_float),
)


def convert_value(value: object, target_type: object) -> object:
    """Convert one SQLite value to the Python type of a target column."""
    if value is None:
        return None
    for rule_type, handler in _CONVERTER_RULES:
        if isinstance(target_type, rule_type):
            return handler(value)
    if isinstance(target_type, (String, Text)):
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return value
    if isinstance(target_type, LargeBinary) and isinstance(value, str):
        return value.encode("utf-8")
    return value


def plan_action(source_count: int, target_count: int) -> str:
    """Decide the transfer action from source and target row counts."""
    if target_count == source_count:
        return "SKIP"
    if target_count > 0:
        return "RELOAD"
    return "LOAD"


def _source_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    """List column names of a SQLite source table."""
    rows = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    return [str(row[1]) for row in rows]


def _count_sqlite(connection: sqlite3.Connection, table: str) -> int:
    """Count rows of a SQLite source table."""
    row = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()  # noqa: S608
    return int(row[0]) if row else 0


def _table_names(connection: sqlite3.Connection) -> set[str]:
    """List user table names of the SQLite source database."""
    rows = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {str(row[0]) for row in rows}


def _has_rowid(connection: sqlite3.Connection, table: str) -> bool:
    """Check whether a SQLite table exposes the rowid pseudo-column."""
    try:
        connection.execute(f'SELECT rowid FROM "{table}" LIMIT 0').fetchone()  # noqa: S608
    except sqlite3.Error:
        return False
    return True


def _count_target(connection: Connection, table: str) -> int:
    """Count rows of a target table."""
    result = connection.execute(text(f'SELECT COUNT(*) FROM "{table}"'))  # noqa: S608
    return int(result.scalar() or 0)


def stream_source_rows(
    source_path: str,
    table: str,
    columns: list[str],
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    """Stream source rows in batches ordered by rowid when available."""
    quoted_cols = ", ".join(f'"{name}"' for name in columns)
    with sqlite3.connect(f"file:{source_path}?mode=ro", uri=True) as connection:
        if _has_rowid(connection, table):
            yield from _stream_by_rowid(connection, table, quoted_cols, columns, batch_size)
        else:
            yield from _stream_by_offset(connection, table, quoted_cols, columns, batch_size)


def _stream_by_rowid(
    connection: sqlite3.Connection,
    table: str,
    quoted_cols: str,
    columns: list[str],
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    """Stream source rows using rowid keyset pagination."""
    last_rowid = 0
    while True:
        cursor = connection.execute(
            f'SELECT rowid, {quoted_cols} FROM "{table}" '  # noqa: S608
            "WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (last_rowid, batch_size),
        )
        rows = cursor.fetchall()
        if not rows:
            return
        last_rowid = int(rows[-1][0])
        yield [dict(zip(columns, row[1:], strict=True)) for row in rows]


def _stream_by_offset(
    connection: sqlite3.Connection,
    table: str,
    quoted_cols: str,
    columns: list[str],
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    """Stream source rows using limit/offset pagination."""
    offset = 0
    while True:
        cursor = connection.execute(
            f'SELECT {quoted_cols} FROM "{table}" LIMIT ? OFFSET ?',  # noqa: S608
            (batch_size, offset),
        )
        rows = cursor.fetchall()
        if not rows:
            return
        offset += len(rows)
        yield [dict(zip(columns, row, strict=True)) for row in rows]


def _column_pairs(source_cols: list[str], target_table: Table) -> list[tuple[str, str]]:
    """Pair source columns to target columns, tolerating legacy UPPER_CASE names."""
    by_lower = {name.lower(): name for name in source_cols}
    pairs: list[tuple[str, str]] = []
    for column in target_table.columns:
        if column.name in source_cols:
            pairs.append((column.name, column.name))
        elif column.name.lower() in by_lower:
            pairs.append((by_lower[column.name.lower()], column.name))
    return pairs


def _convert_batch(
    batch: list[dict[str, Any]],
    target_table: Table,
    pairs: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Convert one batch of source rows to target column types."""
    converted: list[dict[str, Any]] = []
    for row in batch:
        payload: dict[str, Any] = {}
        for source_name, target_name in pairs:
            payload[target_name] = convert_value(row.get(source_name), target_table.columns[target_name].type)
        converted.append(payload)
    return converted


def _order_metas(metas: list[TableMeta], metadata_tables: dict[str, Table]) -> list[TableMeta]:
    """Order tables so referenced parents load before their children."""
    if not metadata_tables:
        return metas
    children: dict[str, set[str]] = {meta.name: set() for meta in metas}
    indegree: dict[str, int] = {meta.name: 0 for meta in metas}
    names = set(children)
    for meta in metas:
        table = metadata_tables.get(meta.name)
        if table is None:
            continue
        for constraint in table.foreign_keys:
            parent = constraint.column.table.name
            if parent in names and parent != meta.name and meta.name not in children[parent]:
                children[parent].add(meta.name)
                indegree[meta.name] += 1
    rank = {meta.name: (meta.level, meta.name) for meta in metas}
    ready = sorted([name for name, degree in indegree.items() if degree == 0], key=lambda n: rank[n])
    ordered: list[str] = []
    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for child in sorted(children[current], key=lambda n: rank[n]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort(key=lambda n: rank[n])
    by_name = {meta.name: meta for meta in metas}
    ordered.extend([name for name in by_name if name not in ordered])
    return [by_name[name] for name in ordered]


def _wrap_pg_params(
    batch: list[dict[str, Any]],
    target_table: Table,
    pairs: list[tuple[str, str]],
) -> tuple[list[tuple[Any, ...]], list[str]]:
    """Convert and wrap one batch into psycopg2 parameter tuples."""
    from psycopg2.extras import Json
    from sqlalchemy.dialects import postgresql

    dialect = postgresql.dialect()
    target_names = [target_name for _, target_name in pairs]
    rows: list[tuple[Any, ...]] = []
    for row in batch:
        params: list[Any] = []
        for source_name, target_name in pairs:
            column_type = target_table.columns[target_name].type
            value = convert_value(row.get(source_name), column_type)
            resolved = column_type.dialect_impl(dialect)
            if isinstance(resolved, postgresql.ARRAY) and isinstance(value, (list, tuple)):
                params.append(list(value))
            elif isinstance(column_type, JSON) and isinstance(value, (dict, list)):
                params.append(Json(value))
            else:
                params.append(value)
        rows.append(tuple(params))
    return rows, target_names


def _is_resume_safe(target_table: Table) -> bool:
    """Check whether conflict-skip resume is safe (PK or unique constraint exists)."""
    from sqlalchemy import UniqueConstraint

    if len(target_table.primary_key.columns) > 0:
        return True
    return any(isinstance(constraint, UniqueConstraint) for constraint in target_table.constraints)


def _load_batches_pg(request: TableTransferRequest, pairs: list[tuple[str, str]], action: str) -> int:
    """Bulk-load batches with psycopg2 execute_values and conflict skipping."""
    from psycopg2 import Error as Psycopg2Error
    from psycopg2.extras import execute_values

    skipped = 0
    name = request.target_table.name
    raw_conn = request.target_engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        try:
            if action == "RELOAD":
                cursor.execute(f'DELETE FROM "{name}"')  # noqa: S608
                raw_conn.commit()
            quoted_cols = ", ".join(f'"{target_name}"' for _, target_name in pairs)
            stmt = f'INSERT INTO "{name}" ({quoted_cols}) VALUES %s ON CONFLICT DO NOTHING'  # noqa: S608
            source_names = [source_name for source_name, _ in pairs]
            batches = stream_source_rows(request.source_path, name, source_names, request.batch_size)
            for batch_index, batch in enumerate(batches, start=1):
                rows, _ = _wrap_pg_params(batch, request.target_table, pairs)
                try:
                    execute_values(cursor, stmt, rows, page_size=len(rows))
                    raw_conn.commit()
                except Psycopg2Error as exc:
                    raw_conn.rollback()
                    detail = str(exc).splitlines()[0][:200]
                    msg = f"batch {batch_index} failed for {name}: {type(exc).__name__}: {detail}"
                    raise RuntimeError(msg) from exc
                rowcount = cursor.rowcount
                if rowcount is not None and rowcount >= 0:
                    skipped += len(rows) - rowcount
                if batch_index % 50 == 0:
                    attempted = batch_index * request.batch_size
                    logger.info("%s: %d batches (%d rows attempted)", name, batch_index, attempted)
        finally:
            cursor.close()
    finally:
        raw_conn.close()
    return skipped


def _load_batches_generic(request: TableTransferRequest, pairs: list[tuple[str, str]], action: str) -> int:
    """Insert converted source batches, returning rows skipped by conflicts."""
    skipped = 0
    name = request.target_table.name
    if action == "RELOAD":
        with request.target_engine.begin() as target_conn:
            target_conn.execute(text(f'DELETE FROM "{name}"'))  # noqa: S608
    insert_stmt = request.target_table.insert()
    source_names = [source_name for source_name, _ in pairs]
    for batch_index, batch in enumerate(
        stream_source_rows(request.source_path, name, source_names, request.batch_size), start=1
    ):
        with request.target_engine.begin() as target_conn:
            result = target_conn.execute(insert_stmt, _convert_batch(batch, request.target_table, pairs))
        rowcount = result.rowcount
        if rowcount is not None and rowcount >= 0:
            skipped += len(batch) - rowcount
        if batch_index % 50 == 0:
            attempted = batch_index * request.batch_size
            logger.info("%s: %d batches (%d rows attempted)", name, batch_index, attempted)
    return skipped


def _load_batches(request: TableTransferRequest, pairs: list[tuple[str, str]], action: str) -> int:
    """Dispatch batch loading to the PostgreSQL fast path when available."""
    if request.target_engine.dialect.name == "postgresql":
        return _load_batches_pg(request, pairs, action)
    return _load_batches_generic(request, pairs, action)


def sync_table(request: TableTransferRequest) -> TableSyncResult:
    """Transfer one table from SQLite to the target database."""
    name = request.target_table.name
    started = time.perf_counter()
    with sqlite3.connect(f"file:{request.source_path}?mode=ro", uri=True) as connection:
        if name not in _table_names(connection):
            return TableSyncResult(name, request.level, "SKIP", 0, 0, 0, 0.0, message="missing in source")
        source_total = _count_sqlite(connection, name)
        source_cols = set(_source_columns(connection, name))
    with request.target_engine.connect() as target_conn:
        target_before = _count_target(target_conn, name)
    action = plan_action(source_total, target_before)
    if not request.apply or action == "SKIP":
        status = "DRY_RUN" if action != "SKIP" else action
        elapsed = time.perf_counter() - started
        return TableSyncResult(name, request.level, status, source_total, target_before, target_before, elapsed)
    is_pg = request.target_engine.dialect.name == "postgresql"
    if action == "RELOAD" and is_pg and _is_resume_safe(request.target_table):
        action = "RESUME"
    pairs = _column_pairs(list(source_cols), request.target_table)
    if not pairs:
        elapsed = time.perf_counter() - started
        return TableSyncResult(
            name,
            request.level,
            "FAILED",
            source_total,
            target_before,
            target_before,
            elapsed,
            0,
            "no shared columns",
        )
    skipped = _load_batches(request, pairs, action)
    with request.target_engine.connect() as target_conn:
        target_after = _count_target(target_conn, name)
    expected_after = source_total - skipped
    status = action if target_after == expected_after else "FAILED"
    message = None
    if skipped:
        message = f"{skipped} duplicate rows collapsed"
    if status == "FAILED":
        mismatch = f"count mismatch: {target_after} != {expected_after}"
        message = f"{message}; {mismatch}" if message else mismatch
    return TableSyncResult(
        name,
        request.level,
        status,
        source_total,
        target_before,
        target_after,
        time.perf_counter() - started,
        skipped,
        message,
    )


def reset_sequences(target_engine: Engine) -> int:
    """Advance PostgreSQL serial sequences past migrated primary key values."""
    if target_engine.dialect.name != "postgresql":
        return 0
    reset = 0
    with target_engine.begin() as connection:
        seq_rows = connection.execute(
            text(
                "SELECT sequence_name FROM information_schema.sequences "
                "WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')"
            ),
        ).fetchall()
        for (seq_name,) in seq_rows:
            table_guess = seq_name.replace("_id_seq", "").replace("_seq", "")
            try:
                if inspect(connection).has_table(table_guess):
                    stmt = text(
                        f"SELECT setval('{seq_name}', "  # noqa: S608
                        f'COALESCE((SELECT MAX(id) FROM "{table_guess}"), 0) + 1, false)'
                    )
                    connection.execute(stmt)
                    reset += 1
            except SQLAlchemyError:
                logger.warning("Skipping sequence reset for %s", seq_name)
    return reset


def _target_metadata_tables() -> dict[str, Table]:
    """Load ORM model metadata for transfer target tables."""
    import src.models  # noqa: F401
    from src.models.base import Base

    return dict(Base.metadata.tables)


def _select_metas(options: SyncOptions) -> list[TableMeta]:
    """Select registry entries honoring level and table filters."""
    metas = sorted(TABLE_REGISTRY, key=lambda meta: (meta.level, meta.name))
    if options.levels:
        metas = [meta for meta in metas if meta.level in options.levels]
    if options.tables:
        wanted = {name.strip() for name in options.tables if name.strip()}
        metas = [meta for meta in metas if meta.name in wanted]
    return [meta for meta in metas if meta.name not in SKIP_TABLES]


def _sync_one(
    meta: TableMeta,
    source_path: str,
    target_engine: Engine,
    metadata_tables: dict[str, Table],
    options: SyncOptions,
) -> TableSyncResult:
    """Transfer one registry table honoring dry-run mode."""
    if options.apply_changes:
        target_table = metadata_tables.get(meta.name)
        if target_table is None:
            return TableSyncResult(meta.name, meta.level, "SKIP", 0, 0, 0, 0.0, message="missing in target schema")
        request = TableTransferRequest(
            source_path, target_engine, target_table, meta.level, options.batch_size, apply=True
        )
        return sync_table(request)
    return _dry_run_table(source_path, target_engine, meta.name, meta.level)


def run_sync(source_url: str, target_url: str, options: SyncOptions) -> SyncReport:
    """Run the transfer across registry tables and build the report."""
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    started = time.perf_counter()
    source_path = _sqlite_path(source_url)
    target_engine = create_engine(target_url)
    metadata_tables = _target_metadata_tables() if options.apply_changes else {}
    metas = _select_metas(options)
    if options.apply_changes:
        metas = _order_metas(metas, metadata_tables)
    results: list[TableSyncResult] = []
    try:
        for meta in metas:
            try:
                result = _sync_one(meta, source_path, target_engine, metadata_tables, options)
            except (sqlite3.Error, SQLAlchemyError, OSError, ValueError, RuntimeError) as exc:
                logger.warning("Table %s failed: %s", meta.name, exc)
                result = TableSyncResult(meta.name, meta.level, "FAILED", 0, 0, None, 0.0, 0, type(exc).__name__)
            results.append(result)
            logger.info(
                "%s: %s (source=%d target_before=%d)",
                result.table_name,
                result.action,
                result.source_count,
                result.target_before,
            )
        sequences = reset_sequences(target_engine) if options.apply_changes else 0
        if sequences:
            logger.info("Reset %d PostgreSQL sequences", sequences)
    finally:
        target_engine.dispose()
    return _build_report(
        started_at, started, "apply" if options.apply_changes else "dry-run", results, apply=options.apply_changes
    )


def _build_report(
    started_at: str,
    started: float,
    mode: str,
    results: list[TableSyncResult],
    *,
    apply: bool,
) -> SyncReport:
    """Aggregate per-table results into a transfer report."""
    synced = sum(1 for item in results if item.action in {"LOAD", "RELOAD", "RESUME"})
    skipped = sum(1 for item in results if item.action in {"SKIP", "DRY_RUN"})
    failed = sum(1 for item in results if item.action == "FAILED")
    return SyncReport(
        started_at=started_at,
        completed_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        total_elapsed_seconds=time.perf_counter() - started,
        mode=mode,
        apply=apply,
        tables_total=len(results),
        tables_synced=synced,
        tables_skipped=skipped,
        tables_failed=failed,
        rows_synced=sum(item.target_after or 0 for item in results if item.action in {"LOAD", "RELOAD", "RESUME"}),
        results=results,
    )


def _dry_run_table(source_path: str, target_engine: Engine, table: str, level: int) -> TableSyncResult:
    """Report planned action for one table without writing."""
    with sqlite3.connect(f"file:{source_path}?mode=ro", uri=True) as connection:
        if table not in _table_names(connection):
            return TableSyncResult(table, level, "SKIP", 0, 0, 0, 0.0, message="missing in source")
        source_total = _count_sqlite(connection, table)
    with target_engine.connect() as target_conn:
        if not inspect(target_conn).has_table(table):
            return TableSyncResult(table, level, "SKIP", source_total, 0, 0, 0.0, message="missing in target schema")
        target_total = _count_target(target_conn, table)
    action = plan_action(source_total, target_total)
    status = "DRY_RUN" if action in {"LOAD", "RELOAD"} else action
    return TableSyncResult(table, level, status, source_total, target_total, target_total, 0.0)


def verify_counts(
    source_url: str, target_url: str, tables: list[str] | None = None, levels: list[int] | None = None
) -> SyncReport:
    """Compare source and target row counts without writing."""
    started_at = time.strftime("%Y-%m-%dT%H:%M:%S")
    started = time.perf_counter()
    source_path = _sqlite_path(source_url)
    target_engine = create_engine(target_url)
    options = SyncOptions(tables=tables, levels=levels)
    results: list[TableSyncResult] = []
    try:
        for meta in _select_metas(options):
            planned = _dry_run_table(source_path, target_engine, meta.name, meta.level)
            status = "LOAD" if planned.source_count == planned.target_before else "FAILED"
            message = None
            if status == "FAILED":
                message = f"mismatch: source={planned.source_count} target={planned.target_before}"
            results.append(
                TableSyncResult(
                    meta.name,
                    meta.level,
                    status,
                    planned.source_count,
                    planned.target_before,
                    planned.target_before,
                    0.0,
                    0,
                    message,
                )
            )
    finally:
        target_engine.dispose()
    return _build_report(started_at, started, "verify", results, apply=False)


def _render_summary(report: SyncReport) -> None:
    """Print the human-readable transfer summary."""
    print(
        f"mode={report.mode} tables={report.tables_total} synced={report.tables_synced} "
        f"skipped={report.tables_skipped} failed={report.tables_failed}"
    )
    for item in report.results:
        if item.action in {"FAILED", "RELOAD"} or (item.action == "DRY_RUN" and item.source_count > 0):
            detail = f"source={item.source_count} target={item.target_before} {item.message or ''}"
            print(f"  {item.table_name}: {item.action} {detail}")


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the transfer CLI and return the process exit code."""
    args = parse_args(argv)
    if args.json:
        logger.setLevel(logging.CRITICAL)
    target_url = args.target_url or os.getenv("DATABASE_URL") or os.getenv("LOCAL_PG_URL", "")
    if not target_url:
        print("A target URL is required (--target-url or DATABASE_URL).")
        return 2
    tables = [name.strip() for name in args.tables.split(",")] if args.tables else None
    options = SyncOptions(batch_size=args.batch_size, apply_changes=args.apply, levels=args.level, tables=tables)
    if args.verify:
        report = verify_counts(args.source_url, target_url, tables, args.level)
    else:
        report = run_sync(args.source_url, target_url, options)
    if args.json:
        print(json.dumps(asdict(report), ensure_ascii=False))
    else:
        _render_summary(report)
    return 0 if report.tables_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
