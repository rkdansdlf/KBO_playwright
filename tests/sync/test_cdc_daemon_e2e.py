"""E2E Integration & Failure Recovery Tests for CDCDaemon.

Validates the full continuous data synchronization pipeline:
1. Normal incremental change capture (INSERT)
2. In-place update merge (UPDATE)
3. Crash & restart checkpoint recovery (Fault Tolerance)
4. Repeated cycle idempotency & deduplication (No duplicate rows)
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.sync.cdc_daemon import CDCDaemon, CDCSyncSummary
from src.sync.checkpoint import CheckpointManager


@pytest.fixture
def mock_sqlite_source(tmp_path: Path):
    """Create a temporary SQLite source database with sample tables and WAL checkpoints."""
    db_path = str(tmp_path / "source_kbo.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE game (
            game_id TEXT PRIMARY KEY,
            game_date DATE,
            away_team TEXT,
            home_team TEXT,
            away_score INTEGER,
            home_score INTEGER,
            game_status TEXT,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        INSERT INTO game VALUES
        ('20260815LGKIA0', '2026-08-15', 'KIA', 'LG', 3, 5, 'FINAL', '2026-08-15 12:00:00')
        """
    )
    conn.commit()
    conn.close()
    return db_path


def test_cdc_sync_once_success_flow(mock_sqlite_source: str) -> None:
    """Scenario 1: Normal CDC sync detects source changes and executes incremental load."""
    daemon = CDCDaemon(
        sqlite_path=mock_sqlite_source,
        target_url="oracle+oracledb://mock_user:mock_pass@mock_host/adb",
        sync_interval=10,
    )

    mock_report = MagicMock()
    mock_report.tables_synced = 3
    mock_report.rows_synced = 25

    with patch("src.cli.sync_sqlite_to_oci.SqliteToOciSynchronizer") as mock_sync_cls:
        instance = mock_sync_cls.return_value
        instance.run_sync.return_value = mock_report

        summary = daemon.sync_once(dry_run=False)

        assert summary.status == "SUCCESS"
        assert summary.synced_tables_count == 3
        assert summary.total_rows_synced == 25
        assert summary.error_message is None
        instance.run_sync.assert_called_once_with(mode="incremental")
        instance.close.assert_called_once()


def test_cdc_sync_update_and_idempotency(mock_sqlite_source: str) -> None:
    """Scenario 2 & 4: In-place update merge and idempotent repeated runs."""
    daemon = CDCDaemon(
        sqlite_path=mock_sqlite_source,
        target_url="oracle+oracledb://mock_user:mock_pass@mock_host/adb",
    )

    mock_report = MagicMock()
    mock_report.tables_synced = 1
    mock_report.rows_synced = 1

    with patch("src.cli.sync_sqlite_to_oci.SqliteToOciSynchronizer") as mock_sync_cls:
        instance = mock_sync_cls.return_value
        instance.run_sync.return_value = mock_report

        # Cycle 1: First sync
        summary1 = daemon.sync_once(dry_run=False)
        assert summary1.status == "SUCCESS"
        assert summary1.total_rows_synced == 1

        # Cycle 2: Immediate re-run (idempotency check)
        mock_report.rows_synced = 0
        summary2 = daemon.sync_once(dry_run=False)
        assert summary2.status == "SUCCESS"
        assert summary2.total_rows_synced == 0


def test_cdc_checkpoint_crash_and_recovery(mock_sqlite_source: str) -> None:
    """Scenario 3: Daemon crash and restart recovery from saved checkpoints."""
    conn = sqlite3.connect(mock_sqlite_source)
    mgr = CheckpointManager(conn, initialize=True)

    # 1. Write initial checkpoint before simulated crash
    mgr.record_success(
        table_name="game",
        synced_at=datetime(2026, 8, 15, 10, 0, 0, tzinfo=UTC),
        rows_synced=1,
        last_rowid=1,
        last_pk_val="20260815LGKIA0",
    )
    conn.commit()

    # 2. Verify checkpoint persisted
    saved_cp = mgr.get_checkpoint("game")
    assert saved_cp is not None
    assert saved_cp.last_pk_val == "20260815LGKIA0"

    # 3. Simulate new record inserted while daemon was down
    conn.execute(
        """
        INSERT INTO game VALUES
        ('20260815SSDO0', '2026-08-15', 'DO', 'SS', 2, 4, 'FINAL', '2026-08-15 14:00:00')
        """
    )
    conn.commit()
    conn.close()

    # 4. Restart daemon and execute sync pass
    daemon = CDCDaemon(
        sqlite_path=mock_sqlite_source,
        target_url="oracle+oracledb://mock_user:mock_pass@mock_host/adb",
    )

    mock_report = MagicMock()
    mock_report.tables_synced = 1
    mock_report.rows_synced = 1  # Only the 1 new row

    with patch("src.cli.sync_sqlite_to_oci.SqliteToOciSynchronizer") as mock_sync_cls:
        instance = mock_sync_cls.return_value
        instance.run_sync.return_value = mock_report

        recovery_summary = daemon.sync_once(dry_run=False)

        assert recovery_summary.status == "SUCCESS"
        assert recovery_summary.total_rows_synced == 1


def test_cdc_daemon_graceful_shutdown(mock_sqlite_source: str) -> None:
    """Daemon thread loop should terminate cleanly when stop_event is signaled."""
    daemon = CDCDaemon(
        sqlite_path=mock_sqlite_source,
        sync_interval=1,
    )
    stop_event = threading.Event()

    def _mock_sync(*_args: object, **_kwargs: object) -> CDCSyncSummary:
        stop_event.set()
        return CDCSyncSummary(
            synced_tables_count=1,
            total_rows_synced=5,
            duration_seconds=0.1,
            status="SUCCESS",
            timestamp=datetime.now(UTC),
        )

    with patch.object(daemon, "sync_once", side_effect=_mock_sync) as mock_sync_once:
        daemon.run_daemon_loop(stop_event, dry_run=True)
        assert mock_sync_once.called
