"""Regression tests for Sync Failure Contracts (S-1, S-2, S-3).

Isolated tests using in-memory SQLite and mock OracleWriter.
No external network, no real Oracle, no .env credentials.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from src.cli.sync.sync_sqlite_to_oci import (
    SyncOptions,
    SyncReport,
    SqliteToOciSynchronizer,
    TableSyncResult,
    main,
)
from src.sync.table_dag import SyncStrategy, TableMeta


def _create_isolated_sync(tmp_path=None) -> tuple[SqliteToOciSynchronizer, sqlite3.Connection]:
    """Create an isolated synchronizer with in-memory SQLite (no .env, no OCI)."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, updated_at TEXT)")
    conn.commit()

    options = SyncOptions(concurrency=1, apply_changes=False)
    sync = SqliteToOciSynchronizer(
        sqlite_path=":memory:",
        oci_url=None,
        tns_admin=None,
        options=options,
    )
    sync.sq_conn = conn
    return sync, conn


# ============================================================================
# S-1: Verify consistency must exit 0 ONLY on full match; MISMATCH -> exit 1
# ============================================================================


class TestVerifyFailureContractS1:
    """S-1: verify mode must return exit code 1 on mismatch or query error."""

    def test_verify_returns_exit_0_when_all_match(self) -> None:
        """When all tables match, verify exits 0."""
        sync, conn = _create_isolated_sync()
        conn.execute("INSERT INTO test_tbl VALUES (1, '2026-01-01')")
        conn.commit()

        mock_writer = MagicMock()
        mock_writer.count_table.return_value = 1
        sync._oracle_writer = mock_writer

        with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
            test_args = ["prog", "--target-url", "oracle+oracledb://fake/srv", "--verify", "--tables", "test_tbl"]
            with patch("sys.argv", test_args):
                exit_code = main()

        assert exit_code == 0
        conn.close()

    def test_verify_returns_exit_1_on_count_mismatch(self) -> None:
        """S-1 defect reproduction: count mismatch must return exit 1, not 0."""
        sync, conn = _create_isolated_sync()
        conn.execute("INSERT INTO test_tbl VALUES (1, '2026-01-01')")
        conn.commit()

        mock_writer = MagicMock()
        mock_writer.count_table.return_value = 999  # Mismatch: SQLite=1, OCI=999
        sync._oracle_writer = mock_writer

        with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
            test_args = ["prog", "--target-url", "oracle+oracledb://fake/srv", "--verify", "--tables", "test_tbl"]
            with patch("sys.argv", test_args):
                exit_code = main()

        assert exit_code == 1, f"Expected exit code 1 on MISMATCH, got {exit_code}"
        conn.close()

    def test_verify_returns_exit_1_on_oracle_query_error(self) -> None:
        """S-1: If count_table raises, verify must report error and exit 1."""
        sync, conn = _create_isolated_sync()
        conn.execute("INSERT INTO test_tbl VALUES (1, '2026-01-01')")
        conn.commit()

        mock_writer = MagicMock()
        mock_writer.count_table.side_effect = OperationalError(
            statement="SELECT COUNT(*)",
            params={},
            orig=Exception("connection refused"),
        )
        sync._oracle_writer = mock_writer

        with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
            test_args = ["prog", "--target-url", "oracle+oracledb://fake/srv", "--verify", "--tables", "test_tbl"]
            with patch("sys.argv", test_args):
                exit_code = main()

        assert exit_code == 1, f"Expected exit code 1 on query error, got {exit_code}"
        conn.close()

    def test_verify_returns_exit_1_on_missing_source_table(self) -> None:
        """S-1: If source table is missing in SQLite, verify must exit 1."""
        sync, conn = _create_isolated_sync()
        # 'non_existent_table' is not in SQLite

        mock_writer = MagicMock()
        mock_writer.count_table.return_value = 0
        sync._oracle_writer = mock_writer

        with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
            test_args = [
                "prog",
                "--target-url",
                "oracle+oracledb://fake/srv",
                "--verify",
                "--tables",
                "non_existent_table",
            ]
            with patch("sys.argv", test_args):
                exit_code = main()

        assert exit_code == 1, f"Expected exit code 1 on missing source table, got {exit_code}"
        conn.close()


# ============================================================================
# S-3: PARTIAL table sync must be reported separately & exit 1 in main()
# ============================================================================


class TestPartialFailureContractS3:
    """S-3: PARTIAL results must be tracked and cause main() to return exit 1."""

    def test_sync_report_tracks_partial_tables_separately(self) -> None:
        """SyncReport must have a tables_partial field distinct from tables_failed."""
        report = SyncReport(
            started_at="2026-09-14T00:00:00",
            completed_at="2026-09-14T00:01:00",
            total_elapsed_seconds=60.0,
            mode="incremental",
            apply=True,
            tables_total=3,
            tables_synced=1,
            tables_failed=1,
            tables_partial=1,
            rows_synced=50,
            results=[],
        )
        assert report.tables_partial == 1
        assert report.tables_failed == 1
        assert report.tables_synced == 1

    def test_main_exits_1_when_only_partial_tables_occur(self) -> None:
        """S-3 defect reproduction: if tables_failed=0 but tables_partial=1, main() must exit 1."""
        sync, conn = _create_isolated_sync()

        # Simulate a report with 0 failed tables, but 1 PARTIAL table
        fake_report = SyncReport(
            started_at="2026-09-14T00:00:00",
            completed_at="2026-09-14T00:01:00",
            total_elapsed_seconds=1.0,
            mode="incremental",
            apply=True,
            tables_total=1,
            tables_synced=0,
            tables_failed=0,
            tables_partial=1,
            rows_synced=5,
            results=[
                TableSyncResult(
                    table_name="test_tbl",
                    level=1,
                    strategy="incremental",
                    candidates_count=10,
                    synced_count=5,
                    error_count=5,
                    elapsed_seconds=1.0,
                    status="PARTIAL",
                )
            ],
        )

        with patch.object(sync, "run_sync", return_value=fake_report):
            with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
                test_args = ["prog", "--target-url", "oracle+oracledb://fake/srv", "--apply"]
                with patch("sys.argv", test_args):
                    exit_code = main()

        assert exit_code == 1, f"Expected exit code 1 for PARTIAL table, got {exit_code}"
        conn.close()

    def test_main_exits_0_on_all_success(self) -> None:
        """Normal execution with all tables succeeding must exit 0."""
        sync, conn = _create_isolated_sync()

        fake_report = SyncReport(
            started_at="2026-09-14T00:00:00",
            completed_at="2026-09-14T00:01:00",
            total_elapsed_seconds=1.0,
            mode="incremental",
            apply=True,
            tables_total=1,
            tables_synced=1,
            tables_failed=0,
            tables_partial=0,
            rows_synced=10,
            results=[
                TableSyncResult(
                    table_name="test_tbl",
                    level=1,
                    strategy="incremental",
                    candidates_count=10,
                    synced_count=10,
                    error_count=0,
                    elapsed_seconds=1.0,
                    status="SUCCESS",
                )
            ],
        )

        with patch.object(sync, "run_sync", return_value=fake_report):
            with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
                test_args = ["prog", "--target-url", "oracle+oracledb://fake/srv", "--apply"]
                with patch("sys.argv", test_args):
                    exit_code = main()

        assert exit_code == 0
        conn.close()

    def test_main_exits_0_on_dry_run(self) -> None:
        """Dry-run without failures must exit 0."""
        sync, conn = _create_isolated_sync()

        fake_report = SyncReport(
            started_at="2026-09-14T00:00:00",
            completed_at="2026-09-14T00:01:00",
            total_elapsed_seconds=1.0,
            mode="incremental",
            apply=False,
            tables_total=1,
            tables_synced=1,
            tables_failed=0,
            tables_partial=0,
            rows_synced=0,
            results=[
                TableSyncResult(
                    table_name="test_tbl",
                    level=1,
                    strategy="incremental",
                    candidates_count=10,
                    synced_count=0,
                    error_count=0,
                    elapsed_seconds=1.0,
                    status="DRY_RUN",
                )
            ],
        )

        with patch.object(sync, "run_sync", return_value=fake_report):
            with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
                test_args = ["prog", "--dry-run"]
                with patch("sys.argv", test_args):
                    exit_code = main()

        assert exit_code == 0
        conn.close()

    def test_main_exits_0_on_noop_empty_sync(self) -> None:
        """When there are 0 candidates, status is SUCCESS and exit code is 0."""
        sync, conn = _create_isolated_sync()

        fake_report = SyncReport(
            started_at="2026-09-14T00:00:00",
            completed_at="2026-09-14T00:01:00",
            total_elapsed_seconds=0.1,
            mode="incremental",
            apply=True,
            tables_total=1,
            tables_synced=1,
            tables_failed=0,
            tables_partial=0,
            rows_synced=0,
            results=[
                TableSyncResult(
                    table_name="test_tbl",
                    level=1,
                    strategy="incremental",
                    candidates_count=0,
                    synced_count=0,
                    error_count=0,
                    elapsed_seconds=0.1,
                    status="SUCCESS",
                    message="No changes to sync.",
                )
            ],
        )

        with patch.object(sync, "run_sync", return_value=fake_report):
            with patch("src.cli.sync.sync_sqlite_to_oci.SqliteToOciSynchronizer", return_value=sync):
                test_args = ["prog", "--target-url", "oracle+oracledb://fake/srv", "--apply"]
                with patch("sys.argv", test_args):
                    exit_code = main()

        assert exit_code == 0
        conn.close()


# ============================================================================
# S-2 Interaction: count_table failure during sync causes table FAILED
# ============================================================================


class TestCountTableSyncInteractionS2:
    """S-2: count_table query failure during sync execution must mark table FAILED."""

    def test_sync_single_table_marks_failed_when_count_table_raises(self) -> None:
        """When writer.count_table raises during sync preparation, table is marked FAILED."""
        sync, conn = _create_isolated_sync()
        conn.execute("INSERT INTO test_tbl VALUES (1, '2026-01-01')")
        conn.commit()

        sync.options.apply_changes = True

        mock_writer = MagicMock()
        mock_writer.truncate_table.return_value = None
        mock_writer.get_columns.return_value = {"ID": "NUMBER", "UPDATED_AT": "VARCHAR2"}
        mock_writer.get_char_sizes.return_value = {}
        mock_writer.get_column_names.return_value = {"ID": "ID", "UPDATED_AT": "UPDATED_AT"}
        mock_writer.get_pk_columns.return_value = ["ID"]
        # count_table raises when checking if target is empty
        mock_writer.count_table.side_effect = OperationalError(
            statement="SELECT COUNT(*)",
            params={},
            orig=Exception("network disconnect"),
        )

        meta = TableMeta("test_tbl", level=1, strategy=SyncStrategy.INCREMENTAL, timestamp_col="updated_at")
        res = sync.sync_single_table(meta, mode="incremental", since_dt=None, season=None, writer=mock_writer)

        assert res.status == "FAILED"
        assert res.error_count > 0
        assert mock_writer.rollback.called
        conn.close()
