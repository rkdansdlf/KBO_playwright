"""High-speed SQLite to Oracle Autonomous Database initial-load CLI.

Features:
- Incremental CDC based on timestamps (updated_at) and checkpoints.
- Native Oracle MERGE INTO for fast bulk upsert.
- Level-based table dependency execution with multi-threading.
- Dry-run by default; use ``--apply`` to persist changes.
- Verification mode for row count consistency checking.
- JSON output for CI/CD and automation pipelines.

Usage:
    python3 -m src.cli.sync_sqlite_to_oci --source-url sqlite:///./data/kbo_dev.db \
        --target-url oracle+oracledb://... --dry-run
    python3 -m src.cli.sync_sqlite_to_oci --source-url sqlite:///./data/kbo_dev.db \
        --target-url oracle+oracledb://... --apply
    python3 -m src.cli.sync_sqlite_to_oci --target-url oracle+oracledb://... --verify --json
"""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import json
import logging
import os
import sqlite3
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from sqlalchemy.engine import make_url

from src.sync.checkpoint import CheckpointManager
from src.sync.oracle_writer import TABLE_COL_OVERRIDE, TABLE_OVERRIDE, OracleWriter
from src.sync.table_dag import TABLE_REGISTRY, SyncStrategy

if TYPE_CHECKING:
    from src.sync.table_dag import TableMeta

_KST = ZoneInfo("Asia/Seoul")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sync_sqlite_to_oci")

_ERR_OCI_CONFIG_REQUIRED = "An Oracle target URL and TNS_ADMIN must be configured for writing to OCI."
DEFAULT_SQLITE_URL = "sqlite:///./data/kbo_dev.db"


@dataclass
class TableSyncResult:
    """Represents the execution result for a single table synchronization."""

    table_name: str
    level: int
    strategy: str
    candidates_count: int
    synced_count: int
    error_count: int
    elapsed_seconds: float
    status: str  # SUCCESS, SKIPPED, FAILED, DRY_RUN
    oci_total_after: int | None = None
    message: str | None = None


@dataclass
class SyncReport:
    """Aggregated synchronization run report."""

    started_at: str
    completed_at: str
    total_elapsed_seconds: float
    mode: str
    apply: bool
    tables_total: int
    tables_synced: int
    tables_failed: int
    rows_synced: int
    results: list[TableSyncResult]


@dataclass
class SyncOptions:
    """Configuration options for synchronizer."""

    batch_size: int = 5000
    commit_every: int = 20000
    concurrency: int = 3
    apply_changes: bool = False


@dataclass
class WriteBatchContext:
    """Context container for table batch write operations."""

    table: str
    query: str
    params: list[object]
    sync_columns: list[str]
    oci_cols: dict[str, str]
    char_sizes: dict[str, int]
    sync_sql: str
    writer: OracleWriter
    pk_columns: list[str] = field(default_factory=list)
    column_names: dict[str, str] = field(default_factory=dict)
    insert_only: bool = False


@dataclass
class PrepareContext:
    """Context container for preparing and executing single table sync."""

    meta: TableMeta
    mode: str
    query: str
    params: list[object]
    sq_cols: list[str]
    total_candidates: int
    writer: OracleWriter
    t0: float


class SqliteToOciSynchronizer:
    """Synchronize verified SQLite tables to Oracle Autonomous Database."""

    def __init__(
        self,
        sqlite_path: str,
        oci_url: str | None,
        tns_admin: str | None,
        wallet_password: str | None = None,
        options: SyncOptions | None = None,
    ) -> None:
        """Initialize SQLite connection, checkpoint manager, and OCI configuration."""
        self.sqlite_path = sqlite_path
        self.oci_url = oci_url
        self.tns_admin = tns_admin
        self.wallet_password = wallet_password
        self.options = options or SyncOptions()

        self.sq_conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
        self.sq_conn.row_factory = sqlite3.Row
        self._sq_lock = threading.Lock()
        self.checkpoint_mgr = CheckpointManager(
            self.sq_conn,
            initialize=self.options.apply_changes,
        )

        self._oracle_writer: OracleWriter | None = None

    def get_writer(self) -> OracleWriter:
        """Get or initialize Oracle writer instance."""
        if self._oracle_writer is None:
            if not self.oci_url or not self.tns_admin:
                raise ValueError(_ERR_OCI_CONFIG_REQUIRED)
            self._oracle_writer = OracleWriter(
                oci_url=self.oci_url,
                tns_admin=self.tns_admin,
                wallet_password=self.wallet_password,
                arraysize=self.options.batch_size,
                prefetchrows=self.options.batch_size,
            )
        return self._oracle_writer

    def close(self) -> None:
        """Close database connections and cleanup resources."""
        if self._oracle_writer:
            self._oracle_writer.close()
        with self._sq_lock:
            self.sq_conn.close()

    def _get_sqlite_columns(self, table: str) -> list[str]:
        with self._sq_lock:
            cur = self.sq_conn.execute(f'PRAGMA table_info("{table}")')
            rows = cur.fetchall()
            return [r[1] if isinstance(r, tuple) else r["name"] for r in rows]

    def _normalize_row_value(self, table: str, row: sqlite3.Row | dict[str, Any], col: str) -> object:
        """Normalize legacy values on row before syncing to OCI."""
        val = row[col] if (isinstance(row, sqlite3.Row) or col in row) else None
        if table == "team_daily_roster" and col == "player_name" and not val:
            player_id = row["player_id"]
            if player_id:
                with self._sq_lock:
                    cur = self.sq_conn.execute("SELECT name FROM player_basic WHERE player_id = ?", (player_id,))
                    p_row = cur.fetchone()
                    if p_row:
                        return p_row[0] if isinstance(p_row, tuple) else p_row["name"]
        if table == "player_movements" and col == "team_code" and not val:
            row_keys = row.keys() if hasattr(row, "keys") else ()
            if "canonical_team_id" in row_keys:
                return row["canonical_team_id"]
        return val

    def _parse_since_clause(self, since_str: str | None) -> datetime | None:
        if not since_str:
            return None
        since_str = since_str.strip().lower()
        now = datetime.now(_KST)
        if since_str.endswith("h"):
            with contextlib.suppress(ValueError):
                hours = int(since_str[:-1])
                return now - timedelta(hours=hours)
        if since_str.endswith("d"):
            with contextlib.suppress(ValueError):
                days = int(since_str[:-1])
                return now - timedelta(days=days)
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d"):
            try:
                dt = datetime.strptime(since_str, fmt).replace(tzinfo=_KST)
            except ValueError:
                continue
            else:
                return dt
        return None

    def build_query(
        self,
        meta: TableMeta,
        mode: str,
        since_dt: datetime | None,
        season: int | None,
    ) -> tuple[str, list[object]]:
        """Build SQLite extraction query based on metadata and sync filters."""
        table = meta.name
        sq_cols = self._get_sqlite_columns(table)
        if not sq_cols:
            return "", []

        where_clauses: list[str] = []
        params: list[object] = []

        if season and "season" in sq_cols:
            where_clauses.append('"season" = ?')
            params.append(season)

        if mode == "incremental" and meta.strategy == SyncStrategy.INCREMENTAL:
            target_time = since_dt
            if target_time is None:
                ckpt = self.checkpoint_mgr.get_checkpoint(table)
                if ckpt and ckpt.last_synced_at:
                    target_time = ckpt.last_synced_at

            if target_time is not None and meta.timestamp_col and meta.timestamp_col in sq_cols:
                time_str = target_time.strftime("%Y-%m-%d %H:%M:%S")
                where_clauses.append(f'"{meta.timestamp_col}" >= ?')
                params.append(time_str)

        sql = f'SELECT * FROM "{table}"'  # noqa: S608
        if where_clauses:
            sql += f" WHERE {' AND '.join(where_clauses)}"

        if meta.natural_keys:
            valid_keys = [k for k in meta.natural_keys if k in sq_cols]
            if valid_keys:
                key_order = ", ".join(f'"{k}"' for k in valid_keys)
                sql += f" ORDER BY {key_order}"

        return sql, params

    def _execute_write_batches(self, ctx: WriteBatchContext) -> tuple[int, int]:
        with self._sq_lock:
            cursor = self.sq_conn.execute(ctx.query, ctx.params)
            all_rows = cursor.fetchall()

        synced_total = 0
        error_total = 0
        batch_size = self.options.batch_size

        for batch_idx, offset in enumerate(range(0, len(all_rows), batch_size)):
            rows = all_rows[offset : offset + batch_size]
            payloads: list[dict[str, object]] = []
            for row in rows:
                p: dict[str, object] = {}
                for i, c in enumerate(ctx.sync_columns):
                    v = self._normalize_row_value(ctx.table, row, c)
                    if c in TABLE_OVERRIDE:
                        v = TABLE_OVERRIDE[c](v)
                    col_override = TABLE_COL_OVERRIDE.get((ctx.table, c))
                    if col_override is not None:
                        v = col_override(v)
                    oci_type = ctx.oci_cols.get(c.upper(), "VARCHAR2")
                    limit = ctx.char_sizes.get(c.upper())
                    p[f"c{i}"] = ctx.writer.convert_value(v, oci_type, limit)
                payloads.append(p)

            success_cnt, err_cnt = ctx.writer.execute_batch(
                ctx.sync_sql,
                payloads,
                ctx.table,
                ctx.sync_columns,
                ctx.pk_columns,
                ctx.oci_cols,
                ctx.column_names,
                insert_only=ctx.insert_only,
            )
            synced_total += success_cnt
            error_total += err_cnt
            if err_cnt > 0:
                logger.warning("[%s] Batch error: %d records failed in batch", ctx.table, err_cnt)

            if self.options.commit_every and ((batch_idx + 1) % self.options.commit_every == 0):
                ctx.writer.commit()

        ctx.writer.commit()
        return synced_total, error_total

    def _prepare_and_sync_table(self, ctx: PrepareContext) -> TableSyncResult:
        table = ctx.meta.name
        oci_cols = ctx.writer.get_columns(table)
        if not oci_cols:
            return TableSyncResult(
                table_name=table,
                level=ctx.meta.level,
                strategy=ctx.meta.strategy.value,
                candidates_count=ctx.total_candidates,
                synced_count=0,
                error_count=1,
                elapsed_seconds=time.monotonic() - ctx.t0,
                status="FAILED",
                message=f"Table '{table}' does not exist in OCI.",
            )

        char_sizes = ctx.writer.get_char_sizes(table)
        oci_pks = ctx.writer.get_pk_columns(table)
        pk_cols = [k.upper() for k in (ctx.meta.natural_keys or [p.lower() for p in oci_pks])]
        if not pk_cols:
            pk_cols = [p.upper() for p in oci_pks]

        oci_lower = {c.lower(): c for c in oci_cols}
        sync_columns = [c for c in ctx.sq_cols if c.lower() in oci_lower]
        if ctx.meta.omit_id and "id" in sync_columns:
            sync_columns = [c for c in sync_columns if c.lower() != "id"]

        if not sync_columns:
            return TableSyncResult(
                table_name=table,
                level=ctx.meta.level,
                strategy=ctx.meta.strategy.value,
                candidates_count=ctx.total_candidates,
                synced_count=0,
                error_count=1,
                elapsed_seconds=time.monotonic() - ctx.t0,
                status="FAILED",
                message="No matching columns between SQLite and OCI.",
            )

        if ctx.mode == "truncate_insert" or (
            ctx.meta.strategy == SyncStrategy.TRUNCATE_INSERT and not ctx.meta.natural_keys
        ):
            ctx.writer.truncate_table(table)
            sync_sql = ctx.writer.build_insert_sql(table, sync_columns)
        else:
            sync_sql = ctx.writer.build_merge_sql(table, sync_columns, pk_cols)

        ctx.writer.set_table_triggers(table, enable=False)
        sync_start_time = datetime.now(_KST)

        batch_ctx = WriteBatchContext(
            table=table,
            query=ctx.query,
            params=ctx.params,
            sync_columns=sync_columns,
            oci_cols=oci_cols,
            char_sizes=char_sizes,
            sync_sql=sync_sql,
            writer=ctx.writer,
        )
        synced_total, error_total = self._execute_write_batches(batch_ctx)

        ctx.writer.set_table_triggers(table, enable=True)
        self.checkpoint_mgr.record_success(table, synced_at=sync_start_time, rows_synced=synced_total)
        oci_count = ctx.writer.count_table(table)

        return TableSyncResult(
            table_name=table,
            level=ctx.meta.level,
            strategy=ctx.meta.strategy.value,
            candidates_count=ctx.total_candidates,
            synced_count=synced_total,
            error_count=error_total,
            elapsed_seconds=time.monotonic() - ctx.t0,
            status="SUCCESS" if error_total == 0 else "PARTIAL",
            oci_total_after=oci_count,
        )

    def _extract_candidates_count(self, query: str, params: list[object]) -> int:
        count_sql = f"SELECT COUNT(*) FROM ({query})"  # noqa: S608
        with self._sq_lock:
            return int(self.sq_conn.execute(count_sql, params).fetchone()[0])

    def _get_extraction_context(
        self,
        meta: TableMeta,
        mode: str,
        since_dt: datetime | None,
        season: int | None,
    ) -> tuple[list[str], str, list[object], str | None]:
        table = meta.name
        sq_cols = self._get_sqlite_columns(table)
        if not sq_cols:
            return [], "", [], f"Table '{table}' does not exist in SQLite."

        query, params = self.build_query(meta, mode, since_dt, season)
        if not query:
            return sq_cols, "", [], "No query generated."

        return sq_cols, query, params, None

    def sync_single_table(
        self,
        meta: TableMeta,
        mode: str,
        since_dt: datetime | None,
        season: int | None,
        writer: OracleWriter | None,
    ) -> TableSyncResult:
        """Synchronize a single table from SQLite to OCI."""
        table = meta.name
        t0 = time.monotonic()

        sq_cols, query, params, skip_reason = self._get_extraction_context(meta, mode, since_dt, season)
        if skip_reason:
            return TableSyncResult(
                table_name=table,
                level=meta.level,
                strategy=meta.strategy.value,
                candidates_count=0,
                synced_count=0,
                error_count=0,
                elapsed_seconds=0.0,
                status="SKIPPED",
                message=skip_reason,
            )

        try:
            total_candidates = self._extract_candidates_count(query, params)
        except (sqlite3.Error, RuntimeError) as e:
            return TableSyncResult(
                table_name=table,
                level=meta.level,
                strategy=meta.strategy.value,
                candidates_count=0,
                synced_count=0,
                error_count=1,
                elapsed_seconds=time.monotonic() - t0,
                status="FAILED",
                message=f"Error counting candidates: {e}",
            )

        if total_candidates == 0:
            return TableSyncResult(
                table_name=table,
                level=meta.level,
                strategy=meta.strategy.value,
                candidates_count=0,
                synced_count=0,
                error_count=0,
                elapsed_seconds=time.monotonic() - t0,
                status="SUCCESS",
                message="No changes to sync.",
            )

        if not self.options.apply_changes or writer is None:
            return TableSyncResult(
                table_name=table,
                level=meta.level,
                strategy=meta.strategy.value,
                candidates_count=total_candidates,
                synced_count=0,
                error_count=0,
                elapsed_seconds=time.monotonic() - t0,
                status="DRY_RUN",
                message=f"Dry-run: {total_candidates} rows pending sync.",
            )

        try:
            prep_ctx = PrepareContext(
                meta=meta,
                mode=mode,
                query=query,
                params=params,
                sq_cols=sq_cols,
                total_candidates=total_candidates,
                writer=writer,
                t0=t0,
            )
            return self._prepare_and_sync_table(prep_ctx)
        except (sqlite3.Error, RuntimeError, ValueError, OSError) as e:
            if writer:
                writer.rollback()
                with contextlib.suppress(Exception):
                    writer.set_table_triggers(table, enable=True)
            self.checkpoint_mgr.record_failure(table, error_msg=str(e))
            logger.exception("[%s] Sync failed", table)
            return TableSyncResult(
                table_name=table,
                level=meta.level,
                strategy=meta.strategy.value,
                candidates_count=total_candidates,
                synced_count=0,
                error_count=total_candidates,
                elapsed_seconds=time.monotonic() - t0,
                status="FAILED",
                message=str(e),
            )

    def _sync_level_tables(
        self,
        group: list[TableMeta],
        mode: str,
        since_dt: datetime | None,
        season: int | None,
        writer: OracleWriter | None,
    ) -> list[TableSyncResult]:
        level_results: list[TableSyncResult] = []
        if self.options.concurrency > 1 and len(group) > 1 and not self.options.apply_changes:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.options.concurrency) as executor:
                futures = {executor.submit(self.sync_single_table, m, mode, since_dt, season, writer): m for m in group}
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    level_results.append(res)
                    logger.info(
                        "[%s] Level %d -> %s (%d candidates, %.2fs)",
                        res.table_name,
                        res.level,
                        res.status,
                        res.candidates_count,
                        res.elapsed_seconds,
                    )
        else:
            for m in group:
                res = self.sync_single_table(m, mode, since_dt, season, writer)
                level_results.append(res)
                logger.info(
                    "[%s] Level %d -> %s (%d synced / %d cand, %.2fs)",
                    res.table_name,
                    res.level,
                    res.status,
                    res.synced_count,
                    res.candidates_count,
                    res.elapsed_seconds,
                )
        return level_results

    def run_sync(
        self,
        tables: list[str] | None = None,
        exclude_tables: list[str] | None = None,
        mode: str = "incremental",
        since: str | None = None,
        season: int | None = None,
    ) -> SyncReport:
        """Run complete database synchronization honoring table DAG levels."""
        t_start = datetime.now(_KST)
        t0 = time.monotonic()

        since_dt = self._parse_since_clause(since)
        writer = self.get_writer() if self.options.apply_changes else None

        exclude_set = set(exclude_tables or [])
        if tables:
            req_set = set(tables)
            target_tables_meta = [t for t in TABLE_REGISTRY if t.name in req_set and t.name not in exclude_set]
        else:
            target_tables_meta = [t for t in TABLE_REGISTRY if t.name not in exclude_set]

        level_groups: dict[int, list[TableMeta]] = {}
        for m in target_tables_meta:
            level_groups.setdefault(m.level, []).append(m)

        all_results: list[TableSyncResult] = []

        logger.info(
            "Starting SQLite to OCI sync: mode=%s, apply=%s, tables=%d, since=%s",
            mode,
            self.options.apply_changes,
            len(target_tables_meta),
            since or "auto_checkpoint",
        )

        for level in sorted(level_groups.keys()):
            group = level_groups[level]
            logger.info("--- Processing Level %d (%d tables) ---", level, len(group))
            res_list = self._sync_level_tables(group, mode, since_dt, season, writer)
            all_results.extend(res_list)

        t_end = datetime.now(_KST)
        total_elapsed = time.monotonic() - t0
        synced_count = sum(r.synced_count for r in all_results)
        failed_tables = sum(1 for r in all_results if r.status == "FAILED")
        success_tables = sum(1 for r in all_results if r.status in ("SUCCESS", "DRY_RUN"))

        report = SyncReport(
            started_at=t_start.isoformat(),
            completed_at=t_end.isoformat(),
            total_elapsed_seconds=round(total_elapsed, 2),
            mode=mode,
            apply=self.options.apply_changes,
            tables_total=len(all_results),
            tables_synced=success_tables,
            tables_failed=failed_tables,
            rows_synced=synced_count,
            results=all_results,
        )

        logger.info(
            "Sync completed in %.2fs: %d rows synced across %d/%d tables (%d failed)",
            total_elapsed,
            synced_count,
            success_tables,
            len(all_results),
            failed_tables,
        )

        return report

    def verify_consistency(self, tables: list[str] | None = None) -> dict[str, object]:
        """Verify row count consistency between SQLite and OCI."""
        writer = self.get_writer()
        target = tables or [t.name for t in TABLE_REGISTRY]
        stats: dict[str, object] = {}

        for table in target:
            if not self._get_sqlite_columns(table):
                stats[table] = {
                    "sqlite_count": None,
                    "oci_count": writer.count_table(table),
                    "diff": None,
                    "is_consistent": False,
                    "status": "MISSING_SOURCE_TABLE",
                }
                continue
            sq_count = self.sq_conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]  # noqa: S608
            oci_count = writer.count_table(table)
            diff = sq_count - oci_count
            match = diff == 0
            stats[table] = {
                "sqlite_count": sq_count,
                "oci_count": oci_count,
                "diff": diff,
                "is_consistent": match,
            }
        return stats


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--source-url",
        default=None,
        help="SQLite source URL (defaults to SQLITE_SOURCE_URL or DATABASE_URL when it is SQLite)",
    )
    parser.add_argument(
        "--target-url",
        default=None,
        help="Oracle target URL (defaults to ORACLE_TARGET_URL, OCI_DB_URL, or Oracle DATABASE_URL)",
    )
    parser.add_argument("--apply", action="store_true", help="write changes to OCI Autonomous DB (default: dry-run)")
    parser.add_argument(
        "--mode",
        choices=["incremental", "full", "truncate_insert"],
        default="incremental",
        help="sync mode (default: incremental)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="sync changes since timestamp (YYYY-MM-DD or 24h, 7d). Default: last checkpoint.",
    )
    parser.add_argument("--dry-run", action="store_true", help="preview changes without writing (default)")
    parser.add_argument("--tables", type=str, default=None, help="comma-separated list of tables to sync")
    parser.add_argument(
        "--exclude-tables", type=str, default=None, help="comma-separated list of tables to exclude from sync"
    )
    parser.add_argument("--season", type=int, default=None, help="filter sync to specific season (e.g. 2026)")
    parser.add_argument("--batch-size", type=int, default=5000, help="batch size for bulk upsert (default: 5000)")
    parser.add_argument("--commit-every", type=int, default=20000, help="commit interval (default: 20000)")
    parser.add_argument("--concurrency", type=int, default=3, help="max concurrent worker threads (default: 3)")
    parser.add_argument("--verify", action="store_true", help="run row count consistency verification")
    parser.add_argument("--reset-checkpoint", type=str, default=None, help="reset checkpoint for table or 'ALL'")
    parser.add_argument("--json", action="store_true", help="output summary in JSON format")
    return parser


def _resolve_source_url(explicit_url: str | None) -> str:
    """Resolve and validate the SQLite source URL for an initial load."""
    configured = explicit_url or os.getenv("SQLITE_SOURCE_URL")
    if configured:
        return configured
    database_url = os.getenv("DATABASE_URL")
    if database_url and database_url.startswith("sqlite:"):
        return database_url
    return DEFAULT_SQLITE_URL


def _resolve_target_url(explicit_url: str | None) -> str | None:
    """Resolve the Oracle target without confusing it with the SQLite source."""
    if explicit_url:
        return explicit_url
    target_url = os.getenv("ORACLE_TARGET_URL")
    if target_url:
        return target_url
    database_url = os.getenv("DATABASE_URL")
    if database_url and database_url.startswith("oracle"):
        return database_url
    return os.getenv("OCI_DB_URL")


def _sqlite_path_from_url(source_url: str) -> str:
    """Return the filesystem path from a SQLite SQLAlchemy URL."""
    parsed = make_url(source_url)
    if parsed.drivername != "sqlite":
        message = f"SQLite source URL required, got {parsed.drivername}"
        raise ValueError(message)
    if not parsed.database:
        message = "SQLite source URL must include a database path"
        raise ValueError(message)
    return parsed.database


def main() -> int:
    """Run the SQLite to Oracle initial-load CLI."""
    parser = _build_arg_parser()
    args = parser.parse_args()
    load_dotenv()

    sqlite_url = _resolve_source_url(args.source_url)
    try:
        sqlite_path = _sqlite_path_from_url(sqlite_url)
    except ValueError as exc:
        parser.error(str(exc))
    oci_url = _resolve_target_url(args.target_url)
    tns_admin = os.getenv("TNS_ADMIN")
    wallet_pwd = os.getenv("OCI_WALLET_PASSWORD")

    opts = SyncOptions(
        batch_size=args.batch_size,
        commit_every=args.commit_every,
        concurrency=args.concurrency,
        apply_changes=args.apply and not args.dry_run,
    )

    sync = SqliteToOciSynchronizer(
        sqlite_path=sqlite_path,
        oci_url=oci_url,
        tns_admin=tns_admin,
        wallet_password=wallet_pwd,
        options=opts,
    )

    try:
        if args.reset_checkpoint:
            if args.reset_checkpoint.upper() == "ALL":
                sync.checkpoint_mgr.reset_checkpoint()
                logger.info("All sync checkpoints reset.")
            else:
                sync.checkpoint_mgr.reset_checkpoint(args.reset_checkpoint)
                logger.info("Checkpoint reset for %s", args.reset_checkpoint)
            return 0

        if args.verify:
            table_list = [t.strip() for t in args.tables.split(",")] if args.tables else None
            verify_res = sync.verify_consistency(table_list)
            if args.json:
                print(json.dumps(verify_res, indent=2))  # noqa: T201
            else:
                print("\n=== Consistency Verification ===")  # noqa: T201
                for tbl, data in verify_res.items():
                    status = "OK" if data["is_consistent"] else "MISMATCH"  # type: ignore[index]
                    sq_cnt = data["sqlite_count"]  # type: ignore[index]
                    oci_cnt = data["oci_count"]  # type: ignore[index]
                    diff_cnt = data["diff"]  # type: ignore[index]
                    print(f"[{status}] {tbl}: SQLite={sq_cnt}, OCI={oci_cnt} (diff={diff_cnt})")  # noqa: T201
            return 0

        tables = [t.strip() for t in args.tables.split(",")] if args.tables else None
        exclude = [t.strip() for t in args.exclude_tables.split(",")] if args.exclude_tables else None

        report = sync.run_sync(
            tables=tables,
            exclude_tables=exclude,
            mode=args.mode,
            since=args.since,
            season=args.season,
        )

        if args.json:
            print(json.dumps(asdict(report), indent=2))  # noqa: T201

        return 1 if report.tables_failed > 0 else 0

    finally:
        sync.close()


if __name__ == "__main__":
    sys.exit(main())
