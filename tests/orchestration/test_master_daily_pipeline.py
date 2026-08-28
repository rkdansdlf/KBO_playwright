"""Unit and integration tests for MasterWorkflowOrchestrator Daily Pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from src.orchestration.master import MasterWorkflowOrchestrator

if TYPE_CHECKING:
    import pytest


def test_build_daily_sync_workflow_structure() -> None:
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()
    stages = orch._topological_sort()
    assert stages == [
        "ingestion",
        "processing",
        "analytics",
        "quality_gate",
        "cloud_sync",
        "notification",
    ]


def test_execute_daily_sync_workflow_dry_run() -> None:
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()
    report = orch.execute_workflow("daily_sync_test", context={"date": "20260401"}, dry_run=True)

    assert report.overall_status == "SUCCESS"
    assert report.total_stages == 6
    assert report.completed_stages == 6
    assert report.failed_stages == 0
    assert report.skipped_stages == 0


@patch("src.cli.run_daily_update.main", return_value={"game_count": 5, "games": [1, 2, 3, 4, 5]})
@patch("src.cli.calculate_standings.StandingsCalculator.calculate_year")
@patch("src.analytics.sabermetrics.SabermetricsEngine.get_league_constants")
@patch("src.cli.calculate_rankings.rebuild_rankings")
@patch("src.services.quality_hub.QualityHub.run_full_audit")
@patch("src.notifications.dispatcher.NotificationDispatcher.dispatch", return_value=MagicMock(is_delivered=True))
def test_execute_daily_sync_workflow_mocked(
    mock_notif: MagicMock,
    mock_qh: MagicMock,
    mock_rank: MagicMock,
    mock_saber: MagicMock,
    mock_stand: MagicMock,
    mock_update: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    mock_qh.return_value = MagicMock(overall_status="PASS")
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()
    report = orch.execute_workflow("daily_sync_full", context={"date": "20260401", "enable_cloud_sync": False})

    assert report.overall_status == "SUCCESS"
    assert report.completed_stages == 6
    assert report.failed_stages == 0
