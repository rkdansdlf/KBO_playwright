"""Unit tests for src.orchestration.dto."""

from __future__ import annotations

from src.orchestration.dto import (
    MasterWorkflowRunReport,
    StageExecutionResult,
    StageExecutionStatus,
    WorkflowStageMeta,
    WorkflowStageType,
)


def test_workflow_stage_type_values() -> None:
    assert WorkflowStageType.INGESTION == "ingestion"
    assert WorkflowStageType.PROCESSING == "processing"
    assert WorkflowStageType.ANALYTICS == "analytics"
    assert WorkflowStageType.QUALITY_GATE == "quality_gate"
    assert WorkflowStageType.SYNC == "sync"
    assert WorkflowStageType.NOTIFICATION == "notification"


def test_stage_execution_status_values() -> None:
    assert StageExecutionStatus.PENDING == "PENDING"
    assert StageExecutionStatus.RUNNING == "RUNNING"
    assert StageExecutionStatus.COMPLETED == "COMPLETED"
    assert StageExecutionStatus.SKIPPED == "SKIPPED"
    assert StageExecutionStatus.FAILED == "FAILED"


def test_workflow_stage_meta_to_dict() -> None:
    meta = WorkflowStageMeta(
        stage_id="analytics_stage",
        stage_name="Compute Advanced Metrics",
        stage_type=WorkflowStageType.ANALYTICS,
        depends_on=["processing_stage"],
    )
    d = meta.to_dict()
    assert d["stage_id"] == "analytics_stage"
    assert d["stage_type"] == "analytics"
    assert d["depends_on"] == ["processing_stage"]


def test_stage_execution_result_to_dict() -> None:
    res = StageExecutionResult(
        stage_id="processing_stage",
        status=StageExecutionStatus.COMPLETED,
        duration_seconds=1.234,
        records_processed=150,
    )
    d = res.to_dict()
    assert d["stage_id"] == "processing_stage"
    assert d["status"] == "COMPLETED"
    assert d["records_processed"] == 150


def test_master_workflow_run_report_to_dict() -> None:
    rep = MasterWorkflowRunReport(
        workflow_id="wf_001",
        total_stages=6,
        completed_stages=6,
        failed_stages=0,
        skipped_stages=0,
        duration_seconds=5.678,
    )
    d = rep.to_dict()
    assert d["workflow_id"] == "wf_001"
    assert d["overall_status"] == "SUCCESS"
    assert d["total_stages"] == 6
