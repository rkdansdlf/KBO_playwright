"""RED regression: required Master DAG failures must not report SUCCESS.

Isolation contract: every external job is replaced by an explicit test
double. No live crawl, no scheduler daemon, no Oracle/production access.
`get_db_session` is stubbed so the protected local DB is never opened.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

from src.orchestration.dto import StageExecutionStatus
from src.orchestration.master import MasterWorkflowOrchestrator


@contextmanager
def _stub_session(*args: Any, **kwargs: Any):
    """Yield a dummy session without touching any real database."""
    yield MagicMock()


def _pass_audit(*args: Any, **kwargs: Any) -> MagicMock:
    return MagicMock(overall_status="PASS")


def _ctx(**overrides: Any) -> dict[str, Any]:
    """Base context with ingestion replaced by an inert test double."""
    base: dict[str, Any] = {
        "date": "20260401",
        "run_main": MagicMock(return_value={"games": [], "game_count": 0}),
    }
    base.update(overrides)
    return base


def _benign_patches():
    """Stub every external stage dependency with a benign test double."""
    return [
        patch("src.db.engine.get_db_session", side_effect=_stub_session),
        patch("src.cli.calculate_standings.StandingsCalculator.calculate_year"),
        patch("src.analytics.sabermetrics.SabermetricsEngine.get_league_constants"),
        patch("src.cli.calculate_rankings.rebuild_rankings"),
        patch(
            "src.services.quality_hub.QualityHub.run_full_audit",
            side_effect=_pass_audit,
        ),
        patch(
            "src.config.manager.ConfigManager.get_feature_flag",
            return_value=False,
        ),
        patch(
            "src.notifications.dispatcher.NotificationDispatcher.dispatch",
            return_value=MagicMock(is_delivered=True),
        ),
    ]


def _execute(ctx: dict[str, Any], monkeypatch, extra=()) -> Any:
    """Execute daily_sync with all externals stubbed, plus test-specific overrides."""
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()
    with ExitStack() as stack:
        for cm in _benign_patches():
            stack.enter_context(cm)
        for cm in extra:
            stack.enter_context(cm)
        return orch.execute_workflow("red_probe", context=ctx)


def test_required_processing_exception_is_failure(monkeypatch) -> None:
    """Processing stage exception must surface as FAILED, not COMPLETED."""
    report = _execute(
        _ctx(),
        monkeypatch,
        extra=[
            patch(
                "src.cli.calculate_standings.StandingsCalculator.calculate_year",
                side_effect=RuntimeError("boom-processing"),
            ),
        ],
    )
    stage = next(r for r in report.stage_results if r.stage_id == "processing")
    assert stage.status == StageExecutionStatus.FAILED
    assert stage.records_processed == 0
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")


def test_required_analytics_exception_is_failure(monkeypatch) -> None:
    """Analytics stage exception must surface as FAILED, not COMPLETED."""
    mock_rank = MagicMock()
    report = _execute(
        _ctx(),
        monkeypatch,
        extra=[
            patch(
                "src.analytics.sabermetrics.SabermetricsEngine.get_league_constants",
                side_effect=RuntimeError("boom-analytics"),
            ),
            patch("src.cli.calculate_rankings.rebuild_rankings", new=mock_rank),
        ],
    )
    stage = next(r for r in report.stage_results if r.stage_id == "analytics")
    assert stage.status == StageExecutionStatus.FAILED
    assert stage.records_processed == 0
    mock_rank.assert_not_called()
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")


def test_quality_hub_audit_exception_is_failure(monkeypatch) -> None:
    """QualityHub audit exception must surface as FAILED, not COMPLETED."""
    report = _execute(
        _ctx(),
        monkeypatch,
        extra=[
            patch(
                "src.services.quality_hub.QualityHub.run_full_audit",
                side_effect=RuntimeError("boom-quality"),
            ),
        ],
    )
    stage = next(r for r in report.stage_results if r.stage_id == "quality_gate")
    assert stage.status == StageExecutionStatus.FAILED
    assert stage.records_processed == 0
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")


def test_enabled_cloud_sync_exception_is_failure(monkeypatch) -> None:
    """Enabled cloud-sync exception must surface as FAILED, not COMPLETED."""
    report = _execute(
        _ctx(),
        monkeypatch,
        extra=[
            patch(
                "src.config.manager.ConfigManager.get_feature_flag",
                return_value=True,
            ),
            patch("src.sync.sync_engine.OciSyncEngine", side_effect=RuntimeError("boom-sync")),
        ],
    )
    stage = next(r for r in report.stage_results if r.stage_id == "cloud_sync")
    assert stage.status == StageExecutionStatus.FAILED
    assert stage.records_processed == 0
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")


def test_required_failure_blocks_all_dependents_and_sync_never_runs(monkeypatch) -> None:
    """After a required failure every dependent stays SKIPPED; sync double runs 0 times."""
    mock_rank = MagicMock()
    mock_audit = MagicMock(return_value=MagicMock(overall_status="PASS"))
    mock_sync = MagicMock()
    mock_notify = MagicMock(return_value=MagicMock(is_delivered=True))
    report = _execute(
        _ctx(),
        monkeypatch,
        extra=[
            patch(
                "src.cli.calculate_standings.StandingsCalculator.calculate_year",
                side_effect=RuntimeError("boom-processing"),
            ),
            patch("src.cli.calculate_rankings.rebuild_rankings", new=mock_rank),
            patch("src.services.quality_hub.QualityHub.run_full_audit", new=mock_audit),
            patch("src.sync.sync_engine.OciSyncEngine", new=mock_sync),
            patch(
                "src.notifications.dispatcher.NotificationDispatcher.dispatch",
                new=mock_notify,
            ),
        ],
    )
    by_id = {r.stage_id: r for r in report.stage_results}
    assert by_id["processing"].status == StageExecutionStatus.FAILED
    for dep in ("analytics", "quality_gate", "cloud_sync", "notification"):
        assert by_id[dep].status == StageExecutionStatus.SKIPPED, dep
    mock_rank.assert_not_called()
    mock_audit.assert_not_called()
    mock_sync.assert_not_called()
    mock_notify.assert_not_called()
    assert mock_sync.call_count == 0
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")


def test_historical_recovery_unimplemented_must_not_report_success(monkeypatch, capsys) -> None:
    """Unimplemented historical stages must not return fake COMPLETED counts."""
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    orch = MasterWorkflowOrchestrator.build_historical_recovery_workflow()
    report = orch.execute_workflow("red_hist", context={})
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")
    for res in report.stage_results:
        assert res.records_processed == 0
        assert res.status != StageExecutionStatus.COMPLETED

    from src.cli.run_workflow import main as wf_main

    exit_code = wf_main(["--workflow", "historical_recovery", "--json"])
    captured = capsys.readouterr()
    assert exit_code != 0
    assert '"overall_status": "SUCCESS"' not in captured.out


def test_config_disabled_vs_execution_failure_are_distinct(monkeypatch) -> None:
    """Disabled sync is SKIPPED; enabled-but-broken sync is FAILED."""
    disabled = _execute(_ctx(enable_cloud_sync=False), monkeypatch)
    disabled_stage = next(r for r in disabled.stage_results if r.stage_id == "cloud_sync")
    assert disabled_stage.status == StageExecutionStatus.SKIPPED
    assert disabled_stage.records_processed == 0

    failed = _execute(
        _ctx(),
        monkeypatch,
        extra=[
            patch(
                "src.config.manager.ConfigManager.get_feature_flag",
                return_value=True,
            ),
            patch("src.sync.sync_engine.OciSyncEngine", side_effect=RuntimeError("boom-sync")),
        ],
    )
    failed_stage = next(r for r in failed.stage_results if r.stage_id == "cloud_sync")
    assert failed_stage.status == StageExecutionStatus.FAILED
    assert failed_stage.records_processed == 0
    assert failed.overall_status in ("FAILED", "PARTIAL_FAILURE")
