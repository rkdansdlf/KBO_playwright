"""Unit and integration tests for MasterWorkflowOrchestrator Daily Pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from src.orchestration.dto import StageExecutionStatus
from src.orchestration.master import PA_BASELINE_FILENAME, MasterWorkflowOrchestrator

from contextlib import contextmanager

if TYPE_CHECKING:
    import pytest


@contextmanager
def _fake_session():
    """Stand in for ``get_db_session`` so the stage needs no database."""
    yield MagicMock()


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


class TestTheDailyQualityStageIsToldWhereItsBaselineIs:
    """The baseline comparison is inert until something hands the hub a file.

    ``QualityHub`` takes ``baseline_path`` as an argument and builds nothing on its
    own, which keeps the API and CLI callers -- neither of which wants a file --
    working unchanged. That leaves the DAG as the one caller that has to supply
    it, and when it does not, the daily gate keeps reporting the same backlog with
    nothing able to say whether it grew.
    """

    def test_the_quality_stage_builds_a_hub_with_the_configured_baseline(self, monkeypatch) -> None:
        from src.orchestration.master import _run_quality_gate

        seen: dict[str, object] = {}

        class _SpyHub:
            def __init__(self, session, *, baseline_path=None):
                seen["baseline_path"] = baseline_path

            def run_full_audit(self, season=None, target_date=None):
                from src.services.quality_hub import UnifiedQualityReport

                return UnifiedQualityReport(timestamp="t", overall_status="PASS", quality_score=100)

        monkeypatch.setattr("src.services.quality_hub.QualityHub", _SpyHub)
        monkeypatch.setattr("src.orchestration.master.get_db_session", _fake_session, raising=False)

        _run_quality_gate({"date": "2026-06-01"})

        assert seen["baseline_path"] is not None, (
            "the daily quality stage built a hub with no baseline, so every run "
            "reports the same backlog and new damage cannot be distinguished"
        )

    def test_an_unset_environment_variable_still_produces_a_path(self, monkeypatch) -> None:
        """The default has to exist or the comparison silently never happens."""
        from src.orchestration.master import _pa_baseline_path

        monkeypatch.delenv("KBO_PA_BASELINE_PATH", raising=False)

        assert _pa_baseline_path() is not None
        assert str(_pa_baseline_path()).endswith(".json")

    def test_the_environment_variable_overrides_the_default(self, monkeypatch, tmp_path) -> None:
        from src.orchestration.master import _pa_baseline_path

        monkeypatch.setenv("KBO_PA_BASELINE_PATH", str(tmp_path / "custom.json"))

        assert _pa_baseline_path() == tmp_path / "custom.json"

    def test_a_directory_is_accepted_so_a_fresh_install_has_somewhere_to_write(self, monkeypatch, tmp_path) -> None:
        """Pointing at a directory is how the first baseline gets written.

        Without it an operator has to know the exact file name and create it by
        hand before the comparison can ever run.
        """
        from src.orchestration.master import _pa_baseline_path

        monkeypatch.setenv("KBO_PA_BASELINE_PATH", str(tmp_path))

        assert _pa_baseline_path() == tmp_path / PA_BASELINE_FILENAME
