"""High-level Orchestration Engine for SQLite to Oracle Autonomous Database synchronization."""

from __future__ import annotations

import concurrent.futures
import logging
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError

from src.sync.checkpoint import CheckpointManager
from src.sync.dto import (
    SyncEngineConfig,
    SyncExecutionMode,
    SyncRunSummary,
    SyncTablePlan,
    SyncVerificationReport,
    TableSyncResult,
)
from src.sync.oracle_writer import OracleWriter
from src.sync.table_dag import SyncStrategy, TableMeta, get_tables_by_level
from src.sync.verifier import SyncConsistencyVerifier

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")


class OciSyncEngine:
    """Orchestrates database synchronization across DAG dependency levels."""

    def __init__(
        self,
        sqlite_conn: sqlite3.Connection,
        oracle_engine: Engine | None = None,
        *,
        config: SyncEngineConfig | None = None,
        checkpoint_manager: CheckpointManager | None = None,
        oracle_writer: OracleWriter | None = None,
    ) -> None:
        """Initialize the sync engine."""
        self.sqlite_conn = sqlite_conn
        self.oracle_engine = oracle_engine
        self.config = config or SyncEngineConfig()

        self.checkpoint_manager = checkpoint_manager or CheckpointManager(
            sqlite_conn,
            initialize=self.config.apply,
        )
        self.oracle_writer = oracle_writer or (OracleWriter(oracle_engine) if oracle_engine else None)
        self.verifier = SyncConsistencyVerifier(sqlite_conn, oracle_engine)
        self._lock = threading.Lock()

    @property
    def mode(self) -> str:
        """Get sync execution mode."""
        return self.config.mode

    @property
    def apply(self) -> bool:
        """Get apply flag."""
        return self.config.apply

    @property
    def concurrency(self) -> int:
        """Get concurrency level."""
        return self.config.concurrency

    def plan_table(self, table_meta: TableMeta) -> SyncTablePlan:
        """Determine candidate rows and dirty status for a table."""
        t_name = table_meta.name
        try:
            total_rows = self.verifier.get_sqlite_row_count(t_name)
            if total_rows == 0:
                return SyncTablePlan(
                    table_name=t_name,
                    level=table_meta.level,
                    strategy=table_meta.strategy.value,
                    candidate_count=0,
                    is_dirty=False,
                    reason="table_empty",
                )

            if self.mode == SyncExecutionMode.FULL or table_meta.strategy == SyncStrategy.SNAPSHOT:
                return SyncTablePlan(
                    table_name=t_name,
                    level=table_meta.level,
                    strategy=table_meta.strategy.value,
                    candidate_count=total_rows,
                    is_dirty=True,
                    reason="full_or_snapshot",
                )

            # Incremental check with checkpoint
            cp = self.checkpoint_manager.get_checkpoint(t_name)
            if not cp or not cp.last_synced_at:
                return SyncTablePlan(
                    table_name=t_name,
                    level=table_meta.level,
                    strategy=table_meta.strategy.value,
                    candidate_count=total_rows,
                    is_dirty=True,
                    reason="no_checkpoint",
                )

            # Check rows newer than checkpoint
            if table_meta.timestamp_col:
                ts_str = cp.last_synced_at.strftime("%Y-%m-%d %H:%M:%S")
                cursor = self.sqlite_conn.execute(
                    f"SELECT COUNT(1) FROM {t_name} WHERE {table_meta.timestamp_col} > ?",  # noqa: S608
                    (ts_str,),
                )
                row = cursor.fetchone()
                dirty_count = int(row[0]) if row else 0
                return SyncTablePlan(
                    table_name=t_name,
                    level=table_meta.level,
                    strategy=table_meta.strategy.value,
                    candidate_count=dirty_count,
                    is_dirty=dirty_count > 0,
                    reason=f"{dirty_count} new/updated rows",
                )

            return SyncTablePlan(
                table_name=t_name,
                level=table_meta.level,
                strategy=table_meta.strategy.value,
                candidate_count=total_rows,
                is_dirty=True,
                reason="no_timestamp_col",
            )
        except (sqlite3.Error, ValueError, TypeError) as exc:
            logger.warning("Failed to plan table %s: %s", t_name, exc)
            return SyncTablePlan(
                table_name=t_name,
                level=table_meta.level,
                strategy=table_meta.strategy.value,
                candidate_count=0,
                is_dirty=False,
                reason=f"error: {exc}",
            )

    def sync_table(self, table_meta: TableMeta, *, dry_run: bool = False) -> TableSyncResult:
        """Synchronize a single table according to its DAG configuration."""
        t_name = table_meta.name
        started = time.perf_counter()
        plan = self.plan_table(table_meta)

        if not plan.is_dirty or plan.candidate_count == 0:
            return TableSyncResult(
                table_name=t_name,
                level=table_meta.level,
                strategy=table_meta.strategy.value,
                candidates_count=plan.candidate_count,
                synced_count=0,
                error_count=0,
                elapsed_seconds=round(time.perf_counter() - started, 3),
                status="SKIPPED",
                message=plan.reason,
            )

        if dry_run or not self.apply or not self.oracle_writer:
            return TableSyncResult(
                table_name=t_name,
                level=table_meta.level,
                strategy=table_meta.strategy.value,
                candidates_count=plan.candidate_count,
                synced_count=plan.candidate_count,
                error_count=0,
                elapsed_seconds=round(time.perf_counter() - started, 3),
                status="DRY_RUN",
                message=f"Would sync {plan.candidate_count} rows ({plan.reason})",
            )

        # Real Execution via OracleWriter
        try:
            cursor = self.sqlite_conn.cursor()
            cursor.execute(f"SELECT * FROM {t_name}")  # noqa: S608
            col_names = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            row_dicts = [dict(zip(col_names, r, strict=False)) for r in rows]

            if not row_dicts:
                return TableSyncResult(
                    table_name=t_name,
                    level=table_meta.level,
                    strategy=table_meta.strategy.value,
                    candidates_count=0,
                    synced_count=0,
                    error_count=0,
                    elapsed_seconds=round(time.perf_counter() - started, 3),
                    status="SKIPPED",
                )

            written = self.oracle_writer.upsert_rows(
                table_name=t_name,
                rows=row_dicts,
                natural_keys=table_meta.natural_keys,
                strategy=table_meta.strategy.value,
            )

            # Update Checkpoint
            now = datetime.now(_KST)
            self.checkpoint_manager.record_success(
                table_name=t_name,
                synced_at=now,
                rows_synced=written,
                last_rowid=len(row_dicts),
            )

            return TableSyncResult(
                table_name=t_name,
                level=table_meta.level,
                strategy=table_meta.strategy.value,
                candidates_count=len(row_dicts),
                synced_count=written,
                error_count=0,
                elapsed_seconds=round(time.perf_counter() - started, 3),
                status="SUCCESS",
            )
        except (sqlite3.Error, SQLAlchemyError, RuntimeError, ValueError, TypeError) as exc:
            logger.exception("Failed to sync table %s", t_name)
            self.checkpoint_manager.record_failure(table_name=t_name, error_msg=str(exc))
            return TableSyncResult(
                table_name=t_name,
                level=table_meta.level,
                strategy=table_meta.strategy.value,
                candidates_count=plan.candidate_count,
                synced_count=0,
                error_count=1,
                elapsed_seconds=round(time.perf_counter() - started, 3),
                status="FAILED",
                message=str(exc),
            )

    def run_full_sync(self, *, dry_run: bool = False) -> SyncRunSummary:
        """Execute full DAG synchronization across all levels sequentially."""
        started_at = datetime.now(_KST).isoformat()
        start_time = time.perf_counter()
        run_id = f"sync_{uuid.uuid4().hex[:8]}"

        levels = get_tables_by_level()
        all_results: list[TableSyncResult] = []

        for level_idx in sorted(levels.keys()):
            table_list = levels[level_idx]
            logger.info("Starting Sync Level %d (%d tables)...", level_idx, len(table_list))

            if self.concurrency > 1 and len(table_list) > 1:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                    futures = [executor.submit(self.sync_table, t, dry_run=dry_run) for t in table_list]
                    all_results.extend(f.result() for f in concurrent.futures.as_completed(futures))
            else:
                all_results.extend(self.sync_table(t, dry_run=dry_run) for t in table_list)

        completed_at = datetime.now(_KST).isoformat()
        total_elapsed = time.perf_counter() - start_time

        synced_count = sum(1 for r in all_results if r.status in {"SUCCESS", "DRY_RUN"})
        skipped_count = sum(1 for r in all_results if r.status == "SKIPPED")
        failed_count = sum(1 for r in all_results if r.status == "FAILED")
        total_rows = sum(r.synced_count for r in all_results)

        return SyncRunSummary(
            run_id=run_id,
            started_at=started_at,
            completed_at=completed_at,
            total_elapsed_seconds=total_elapsed,
            mode=self.mode,
            apply=self.apply and not dry_run,
            tables_total=len(all_results),
            tables_synced=synced_count,
            tables_skipped=skipped_count,
            tables_failed=failed_count,
            total_rows_synced=total_rows,
            table_results=all_results,
        )

    def verify_consistency(self) -> SyncVerificationReport:
        """Verify data consistency between SQLite source and Oracle target."""
        return self.verifier.verify_all()
