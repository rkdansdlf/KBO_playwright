"""Unit tests for src.sync.dto."""

from __future__ import annotations

from src.sync.dto import (
    ConsistencyCheckItem,
    SyncExecutionMode,
    SyncRunSummary,
    SyncTablePlan,
    SyncVerificationReport,
    TableSyncResult,
)


def test_sync_execution_mode_values() -> None:
    assert SyncExecutionMode.INCREMENTAL == "incremental"
    assert SyncExecutionMode.FULL == "full"
    assert SyncExecutionMode.SNAPSHOT == "snapshot"
    assert SyncExecutionMode.VERIFY_ONLY == "verify_only"


def test_sync_table_plan_to_dict() -> None:
    plan = SyncTablePlan(
        table_name="game",
        level=1,
        strategy="incremental",
        candidate_count=50,
        is_dirty=True,
        reason="50 new rows",
    )
    d = plan.to_dict()
    assert d["table_name"] == "game"
    assert d["level"] == 1
    assert d["candidate_count"] == 50
    assert d["is_dirty"] is True


def test_table_sync_result_to_dict() -> None:
    res = TableSyncResult(
        table_name="player_basic",
        level=0,
        strategy="incremental",
        candidates_count=100,
        synced_count=100,
        error_count=0,
        elapsed_seconds=0.45,
        status="SUCCESS",
    )
    d = res.to_dict()
    assert d["table_name"] == "player_basic"
    assert d["synced_count"] == 100
    assert d["status"] == "SUCCESS"


def test_sync_run_summary_to_dict() -> None:
    summary = SyncRunSummary(
        run_id="run_123",
        started_at="2026-08-23T04:00:00",
        completed_at="2026-08-23T04:02:00",
        total_elapsed_seconds=120.5,
        mode="incremental",
        apply=True,
        tables_total=35,
        tables_synced=30,
        tables_skipped=5,
        tables_failed=0,
        total_rows_synced=5000,
    )
    d = summary.to_dict()
    assert d["run_id"] == "run_123"
    assert d["total_rows_synced"] == 5000
    assert d["tables_synced"] == 30


def test_consistency_check_item_to_dict() -> None:
    item = ConsistencyCheckItem(
        table_name="game",
        level=1,
        sqlite_count=1000,
        oci_count=1000,
        diff=0,
        status="MATCH",
    )
    d = item.to_dict()
    assert d["table_name"] == "game"
    assert d["diff"] == 0
    assert d["status"] == "MATCH"


def test_sync_verification_report_to_dict() -> None:
    report = SyncVerificationReport(
        timestamp="2026-08-23T04:00:00",
        overall_status="PASS",
        tables_checked=35,
        matching_tables=35,
        mismatched_tables=0,
        error_tables=0,
        details=[],
    )
    d = report.to_dict()
    assert d["overall_status"] == "PASS"
    assert d["matching_tables"] == 35
