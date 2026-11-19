"""Unit and integration tests for MasterWorkflowOrchestrator Daily Pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from src.orchestration.dto import StageExecutionStatus
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
    assert report.completed_stages == 5
    assert report.failed_stages == 0
    assert report.skipped_stages == 1
    # Ensure the cloud_sync stage is the one skipped
    cloud_sync_result = next(r for r in report.stage_results if r.stage_id == "cloud_sync")
    assert cloud_sync_result.status == StageExecutionStatus.SKIPPED


@patch(
    "src.cli.run_daily_update.main",
    return_value={
        "game_count": 5,
        "games": [1, 2, 3, 4, 5],
        "derived_refresh": ["standings", "matchups", "stat_rankings"],
    },
)
@patch("src.cli.calculate_standings.StandingsCalculator.calculate_year")
@patch("src.analytics.sabermetrics.SabermetricsEngine.get_league_constants")
@patch("src.cli.calculate_rankings.rebuild_rankings")
@patch("src.services.quality_hub.QualityHub.run_full_audit")
@patch("src.notifications.dispatcher.NotificationDispatcher.dispatch", return_value=MagicMock(is_delivered=True))
def test_derived_stats_are_not_recomputed_when_ingestion_did_them(
    mock_notif: MagicMock,
    mock_qh: MagicMock,
    mock_rank: MagicMock,
    mock_saber: MagicMock,
    mock_stand: MagicMock,
    mock_update: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_daily_update step 8 already owns standings/rankings; the DAG must not redo them.

    Both computations are UPSERTs, so a double run is invisible in the data but
    costs two extra full-season calculations on every nightly pipeline.
    """
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    mock_qh.return_value = MagicMock(overall_status="PASS")
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()

    report = orch.execute_workflow(
        "daily_sync_derived_once",
        context={"date": "20260401", "enable_cloud_sync": False},
    )

    assert report.overall_status == "SUCCESS"
    mock_stand.assert_not_called()
    mock_rank.assert_not_called()
    processing = next(r for r in report.stage_results if r.stage_id == "processing")
    analytics = next(r for r in report.stage_results if r.stage_id == "analytics")
    assert processing.status == StageExecutionStatus.COMPLETED
    assert analytics.status == StageExecutionStatus.COMPLETED
    assert processing.artifacts.get("skipped_reason") == "already_refreshed_by_ingestion"
    assert analytics.artifacts.get("skipped_reason") == "already_refreshed_by_ingestion"


@patch("src.cli.run_daily_update.main", return_value={"game_count": 5, "games": [1, 2, 3, 4, 5]})
@patch("src.cli.calculate_standings.StandingsCalculator.calculate_year")
@patch("src.analytics.sabermetrics.SabermetricsEngine.get_league_constants")
@patch("src.cli.calculate_rankings.rebuild_rankings")
@patch("src.services.quality_hub.QualityHub.run_full_audit")
@patch("src.notifications.dispatcher.NotificationDispatcher.dispatch", return_value=MagicMock(is_delivered=True))
def test_missing_derived_refresh_keeps_the_dag_fallback(
    mock_notif: MagicMock,
    mock_qh: MagicMock,
    mock_rank: MagicMock,
    mock_saber: MagicMock,
    mock_stand: MagicMock,
    mock_update: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ingestion did not report a derived refresh, the DAG still computes it.

    ``run_daily_update`` marks every step ``allow_failure=True``; if step 8 was
    skipped or failed, treating an absent report as "already done" would leave
    the season tables stale with no second path to refresh them.
    """
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    mock_qh.return_value = MagicMock(overall_status="PASS")
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()

    report = orch.execute_workflow(
        "daily_sync_derived_fallback",
        context={"date": "20260401", "enable_cloud_sync": False},
    )

    assert report.overall_status == "SUCCESS"
    mock_stand.assert_called_once()
    mock_rank.assert_called_once()
