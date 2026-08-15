"""Continuous Data Capture (CDC) Daemon for SQLite to OCI Synchronization.

Runs in the background, continuously monitoring SQLite WAL/table checkpoints
and propagating incremental data changes to Oracle Cloud at configured intervals.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CDCSyncSummary:
    """Summary of a single CDC synchronization cycle."""

    synced_tables_count: int
    total_rows_synced: int
    duration_seconds: float
    status: str  # 'SUCCESS', 'SKIPPED', 'ERROR'
    timestamp: datetime
    error_message: str | None = None


class CDCDaemon:
    """Background daemon continuously syncing modified data to OCI."""

    def __init__(
        self,
        sqlite_path: str = "./data/kbo_dev.db",
        target_url: str | None = None,
        tns_admin: str | None = None,
        sync_interval: int = 60,
    ) -> None:
        """Initialize CDC daemon with database endpoints."""
        self.sqlite_path = sqlite_path
        self.target_url = target_url or os.getenv("DATABASE_URL")
        self.tns_admin = tns_admin or os.getenv("TNS_ADMIN")
        self.sync_interval = sync_interval

    def sync_once(self, *, dry_run: bool = False) -> CDCSyncSummary:
        """Execute a single incremental CDC synchronization pass."""
        t0 = time.perf_counter()
        now = datetime.now(UTC)

        try:
            from src.cli.sync_sqlite_to_oci import SqliteToOciSynchronizer, SyncOptions

            options = SyncOptions(
                apply_changes=not dry_run,
            )
            sync = SqliteToOciSynchronizer(
                sqlite_path=self.sqlite_path,
                oci_url=self.target_url,
                tns_admin=self.tns_admin,
                options=options,
            )
            try:
                report = sync.run_sync(mode="incremental")
            finally:
                sync.close()

            duration = round(time.perf_counter() - t0, 3)

            return CDCSyncSummary(
                synced_tables_count=report.tables_synced,
                total_rows_synced=report.rows_synced,
                duration_seconds=duration,
                status="SUCCESS",
                timestamp=now,
            )
        except Exception as exc:
            duration = round(time.perf_counter() - t0, 3)
            logger.exception("[CDCDaemon] Sync cycle failed")
            return CDCSyncSummary(
                synced_tables_count=0,
                total_rows_synced=0,
                duration_seconds=duration,
                status="ERROR",
                timestamp=now,
                error_message=str(exc),
            )

    def run_daemon_loop(
        self,
        stop_event: threading.Event,
        *,
        dry_run: bool = False,
    ) -> None:
        """Run continuous sync loop until stop_event is set."""
        logger.info("[CDCDaemon] Starting continuous sync loop (interval=%ds)", self.sync_interval)
        while not stop_event.is_set():
            summary = self.sync_once(dry_run=dry_run)
            logger.info(
                "[CDCDaemon] Cycle finished: status=%s rows=%d duration=%.2fs",
                summary.status,
                summary.total_rows_synced,
                summary.duration_seconds,
            )
            for _ in range(self.sync_interval):
                if stop_event.is_set():
                    break
                time.sleep(1)

        logger.info("[CDCDaemon] Daemon stopped gracefully.")


__all__ = ["CDCDaemon", "CDCSyncSummary"]
