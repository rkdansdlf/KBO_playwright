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
