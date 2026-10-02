"""Unit tests for scheduler daily crawl using MasterWorkflowOrchestrator DAG."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from src.orchestration.dto import MasterWorkflowRunReport
from src.scheduler.jobs.daily import crawl_daily_games

if TYPE_CHECKING:
    import pytest


@patch("src.orchestration.master.MasterWorkflowOrchestrator.execute_workflow")
def test_crawl_daily_games_uses_dag_orchestrator(
    mock_execute: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DAILY_USE_DAG_ORCHESTRATOR", "1")
    report = MasterWorkflowRunReport(
        workflow_id="daily_sync_20260401",
        total_stages=6,
        completed_stages=6,
        failed_stages=0,
        skipped_stages=0,
        duration_seconds=1.23,
        overall_status="SUCCESS",
    )
    mock_execute.return_value = report

    mock_alert = MagicMock()
    monkeypatch.setattr("src.scheduler.jobs.daily.alert_success", mock_alert)
    monkeypatch.setattr("src.scheduler.alerting.alert_success", mock_alert)
    if "src.scheduler" in sys.modules:
        monkeypatch.setattr(sys.modules["src.scheduler"], "alert_success", mock_alert, raising=False)
    if "scripts.scheduler" in sys.modules:
        monkeypatch.setattr(sys.modules["scripts.scheduler"], "alert_success", mock_alert, raising=False)

    crawl_daily_games()
    mock_execute.assert_called_once()
    mock_alert.assert_called_once()


def _partial_report(failed_stages: list[str]) -> MasterWorkflowRunReport:
    from src.orchestration.dto import StageExecutionResult, StageExecutionStatus

    stages = [
        StageExecutionResult(stage_id="ingestion", status=StageExecutionStatus.COMPLETED),
        StageExecutionResult(stage_id="processing", status=StageExecutionStatus.COMPLETED),
        StageExecutionResult(stage_id="analytics", status=StageExecutionStatus.COMPLETED),
    ]
    stages.extend(
        StageExecutionResult(
            stage_id=stage_id,
            status=StageExecutionStatus.FAILED,
            error_message="quality score 65/100",
        )
        for stage_id in failed_stages
    )
    completed = 3
    failed = len(failed_stages)
    return MasterWorkflowRunReport(
        workflow_id="daily_sync_20260925",
        total_stages=6,
        completed_stages=completed,
        failed_stages=failed,
        skipped_stages=failed + 1,
        duration_seconds=1.0,
        overall_status="PARTIAL_FAILURE",
        stage_results=stages,
    )


@patch("src.orchestration.master.MasterWorkflowOrchestrator.execute_workflow")
def test_quality_gate_only_failure_is_warning_not_failure(
    mock_execute: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A quality-gate-only failure must not block dependent jobs.

    Regression for the prod outage: ``quality_gate`` failing on a stale
    ``team_season_*`` aggregate was recorded as FAILURE, which made
    ``_can_run_job`` skip every dependent job.
    """
    from src.scheduler.jobs.daily import JobStatus, _JOB_REGISTRY, clear_job_registry

    monkeypatch.setenv("DAILY_USE_DAG_ORCHESTRATOR", "1")
    mock_execute.return_value = _partial_report(["quality_gate"])
    monkeypatch.setattr("src.scheduler.jobs.daily._publish_quality_incident", MagicMock())

    clear_job_registry()
    try:
        crawl_daily_games()
        assert _JOB_REGISTRY["crawl_daily_games"].status is JobStatus.WARNING
    finally:
        clear_job_registry()


@patch("src.orchestration.master.MasterWorkflowOrchestrator.execute_workflow")
def test_quality_gate_plus_other_failure_stays_failure(
    mock_execute: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real stage failure must still be FAILURE, warning or not."""
    from src.scheduler.jobs.daily import JobStatus, _JOB_REGISTRY, clear_job_registry

    monkeypatch.setenv("DAILY_USE_DAG_ORCHESTRATOR", "1")
    mock_execute.return_value = _partial_report(["quality_gate", "ingestion"])
    monkeypatch.setattr("src.scheduler.jobs.daily._publish_quality_incident", MagicMock())

    clear_job_registry()
    try:
        crawl_daily_games()
        assert _JOB_REGISTRY["crawl_daily_games"].status is JobStatus.FAILURE
    finally:
        clear_job_registry()
