"""Unit tests for src.orchestration.master."""

from __future__ import annotations

import pytest

from src.orchestration.dto import (
    StageExecutionResult,
    StageExecutionStatus,
    WorkflowStageMeta,
    WorkflowStageType,
)
from src.orchestration.master import MasterWorkflowOrchestrator


def test_topological_sort_linear() -> None:
    orch = MasterWorkflowOrchestrator()
    orch.register_stage(
        WorkflowStageMeta("s1", "Stage 1", WorkflowStageType.INGESTION),
        lambda _ctx: StageExecutionResult("s1", StageExecutionStatus.COMPLETED),
    )
    orch.register_stage(
        WorkflowStageMeta("s2", "Stage 2", WorkflowStageType.PROCESSING, ["s1"]),
        lambda _ctx: StageExecutionResult("s2", StageExecutionStatus.COMPLETED),
    )
    orch.register_stage(
        WorkflowStageMeta("s3", "Stage 3", WorkflowStageType.ANALYTICS, ["s2"]),
        lambda _ctx: StageExecutionResult("s3", StageExecutionStatus.COMPLETED),
    )

    order = orch._topological_sort()
    assert order == ["s1", "s2", "s3"]


def test_topological_sort_circular_raises_error() -> None:
    orch = MasterWorkflowOrchestrator()
    orch.register_stage(
        WorkflowStageMeta("s1", "Stage 1", WorkflowStageType.INGESTION, ["s2"]),
        lambda _ctx: StageExecutionResult("s1", StageExecutionStatus.COMPLETED),
    )
    orch.register_stage(
        WorkflowStageMeta("s2", "Stage 2", WorkflowStageType.PROCESSING, ["s1"]),
        lambda _ctx: StageExecutionResult("s2", StageExecutionStatus.COMPLETED),
    )

    with pytest.raises(ValueError, match="Circular dependency"):
        orch._topological_sort()


def test_execute_workflow_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "1")
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()
    report = orch.execute_workflow("daily_sync_test")

    assert report.overall_status == "SUCCESS"
    assert report.total_stages == 6
    assert report.completed_stages == 6
    assert report.failed_stages == 0
    assert report.skipped_stages == 0


def test_execute_workflow_failure_cascade() -> None:
    orch = MasterWorkflowOrchestrator()
    orch.register_stage(
        WorkflowStageMeta("s1", "Stage 1", WorkflowStageType.INGESTION),
        lambda _ctx: StageExecutionResult("s1", StageExecutionStatus.FAILED, error_message="Network error"),
    )
    orch.register_stage(
        WorkflowStageMeta("s2", "Stage 2", WorkflowStageType.PROCESSING, ["s1"]),
        lambda _ctx: StageExecutionResult("s2", StageExecutionStatus.COMPLETED),
    )

    report = orch.execute_workflow("cascade_test")
    assert report.overall_status == "FAILED"
    assert report.failed_stages == 1
    assert report.skipped_stages == 1


def test_execute_workflow_dry_run() -> None:
    orch = MasterWorkflowOrchestrator.build_historical_recovery_workflow()
    report = orch.execute_workflow("hist_dry_run", dry_run=True)

    assert report.overall_status == "SUCCESS"
    assert report.total_stages == 4
    assert report.completed_stages == 4
