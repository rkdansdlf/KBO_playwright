"""Unit tests for src.maintenance.dto."""

from __future__ import annotations

from src.maintenance.dto import (
    MaintenanceRunReport,
    MaintenanceTaskMeta,
    MaintenanceTaskResult,
    MaintenanceTaskType,
)


def test_maintenance_task_type_values() -> None:
    assert MaintenanceTaskType.PA_AUDIT == "pa_audit"
    assert MaintenanceTaskType.NULL_PLAYER_IDS == "null_player_ids"
    assert MaintenanceTaskType.DATA_CLEANUP == "data_cleanup"
    assert MaintenanceTaskType.WAL_CHECKPOINT == "wal_checkpoint"
    assert MaintenanceTaskType.CUSTOM == "custom"


def test_maintenance_task_meta_to_dict() -> None:
    meta = MaintenanceTaskMeta(
        task_name="pa_formula_audit",
        task_type=MaintenanceTaskType.PA_AUDIT,
        description="Audit and fix PA formula violations",
        safe_mode_supported=True,
    )
    d = meta.to_dict()
    assert d["task_name"] == "pa_formula_audit"
    assert d["task_type"] == "pa_audit"
    assert d["safe_mode_supported"] is True


def test_maintenance_task_result_to_dict() -> None:
    res = MaintenanceTaskResult(
        task_name="pa_formula_audit",
        status="SUCCESS",
        rows_affected=5,
        duration_seconds=0.123,
    )
    d = res.to_dict()
    assert d["task_name"] == "pa_formula_audit"
    assert d["status"] == "SUCCESS"
    assert d["rows_affected"] == 5
    assert d["duration_seconds"] == 0.123


def test_maintenance_run_report_to_dict() -> None:
    report = MaintenanceRunReport(
        total_tasks=3,
        successful_tasks=3,
        failed_tasks=0,
        total_rows_affected=15,
        duration_seconds=0.456,
        results=[],
        started_at="2026-08-26T22:00:00Z",
        completed_at="2026-08-26T22:00:01Z",
    )
    d = report.to_dict()
    assert d["total_tasks"] == 3
    assert d["successful_tasks"] == 3
    assert d["total_rows_affected"] == 15
