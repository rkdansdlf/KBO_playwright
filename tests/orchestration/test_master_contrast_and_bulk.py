"""Batch-2 contrasts: happy-path sync reachability, FAIL-verdict blocking,
stub-omission tripwire guard, and bulk_load placeholder false-success removal.

Isolation contract: every external job is replaced by an explicit test
double. `get_db_session` is stubbed so the protected local DB is never
opened. No live crawl, no scheduler daemon, no Oracle/production access.

Guard scope (explicit, no broader claim): the tripwire patches exactly the
10 handler-level external call sites of `src/orchestration/master.py`. It is
NOT a whole-network block (raw sockets/httpx/Playwright remain unpatched).
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

from src.orchestration.dto import StageExecutionStatus
from src.orchestration.master import MasterWorkflowOrchestrator


class TripwireError(RuntimeError):
    """Raised when business code reaches an external entry without a stub."""


GUARD_TARGETS = [
    "src.cli.run_daily_update.main",
    "src.db.engine.get_db_session",
    "src.cli.calculate_standings.StandingsCalculator.calculate_year",
    "src.analytics.sabermetrics.SabermetricsEngine.get_league_constants",
    "src.cli.calculate_rankings.rebuild_rankings",
    "src.services.quality_hub.QualityHub.run_full_audit",
    "src.config.manager.ConfigManager.get_feature_flag",
    "src.sync.sync_engine.OciSyncEngine",
    "src.notifications.dispatcher.NotificationDispatcher.dispatch",
    "src.services.bulk_loader.BulkChunkLoader",
]


@contextmanager
def install_guard(tripwire: list[str]):
    """Patch every external entry to record-then-raise on any unstubbed call."""

    def _raiser(name: str):
        def _hit(*args: Any, **kwargs: Any) -> Any:
            tripwire.append(name)
            message = f"unstubbed external reached: {name}"
            raise TripwireError(message)

        return _hit

    with ExitStack() as stack:
        for target in GUARD_TARGETS:
            stack.enter_context(patch(target, side_effect=_raiser(target)))
        yield tripwire


@contextmanager
def _stub_session(*args: Any, **kwargs: Any):
    """Yield a dummy session without touching any real database."""
    yield MagicMock()


def _pass_audit(*args: Any, **kwargs: Any) -> MagicMock:
    return MagicMock(overall_status="PASS")


def _daily_ctx(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "date": "20260401",
        "run_main": MagicMock(return_value={"games": [], "game_count": 0}),
    }
    base.update(overrides)
    return base


def _benign_daily_patches(extra_sync_rows: int | None = None):
    """Benign doubles for every daily_sync external (override guard)."""
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
            return_value=True,
        ),
        patch(
            "src.notifications.dispatcher.NotificationDispatcher.dispatch",
            return_value=MagicMock(is_delivered=True),
        ),
    ]


def _execute_daily(ctx: dict[str, Any], monkeypatch, extra=()) -> Any:
    """Execute daily_sync under guard + benign doubles; fail on any tripwire hit.

    The benign set covers every daily_sync external so the test stays within
    isolation bounds regardless of the extra overrides supplied by a caller.
    """
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    tripwire: list[str] = []
    orch = MasterWorkflowOrchestrator.build_daily_sync_workflow()
    with ExitStack() as stack:
        stack.enter_context(install_guard(tripwire))
        for cm in _benign_daily_patches():
            stack.enter_context(cm)
        for cm in extra:
            stack.enter_context(cm)
        report = orch.execute_workflow("batch2_probe", context=ctx)
    assert tripwire == [], f"test isolation breach: {tripwire}"
    return report


# ---------------------------------------------------------------------------
# A3 contrasts
# ---------------------------------------------------------------------------


def test_enabled_happy_path_reaches_sync(monkeypatch) -> None:
    """With the same enabled config, the happy path reaches sync and notifies."""
    mock_sync_cls = MagicMock()
    mock_sync_cls.return_value.sync_incremental.return_value = MagicMock(total_synced_rows=7)
    mock_notify = MagicMock(return_value=MagicMock(is_delivered=True))
    report = _execute_daily(
        _daily_ctx(),
        monkeypatch,
        extra=[
            patch("src.sync.sync_engine.OciSyncEngine", new=mock_sync_cls),
            patch(
                "src.notifications.dispatcher.NotificationDispatcher.dispatch",
                new=mock_notify,
            ),
        ],
    )
    by_id = {r.stage_id: r for r in report.stage_results}
    assert by_id["cloud_sync"].status == StageExecutionStatus.COMPLETED
    assert by_id["cloud_sync"].records_processed == 7
    mock_sync_cls.assert_called_once_with()
    mock_notify.assert_called_once()
    assert report.overall_status == "SUCCESS"
    assert report.completed_stages == 6


def test_quality_fail_verdict_blocks_downstream(monkeypatch) -> None:
    """A QualityHub FAIL verdict (not exception) blocks sync and notification."""
    mock_sync_cls = MagicMock()
    mock_notify = MagicMock(return_value=MagicMock(is_delivered=True))
    report = _execute_daily(
        _daily_ctx(),
        monkeypatch,
        extra=[
            patch(
                "src.services.quality_hub.QualityHub.run_full_audit",
                return_value=MagicMock(overall_status="FAIL"),
            ),
            patch("src.sync.sync_engine.OciSyncEngine", new=mock_sync_cls),
            patch(
                "src.notifications.dispatcher.NotificationDispatcher.dispatch",
                new=mock_notify,
            ),
        ],
    )
    by_id = {r.stage_id: r for r in report.stage_results}
    assert by_id["quality_gate"].status == StageExecutionStatus.FAILED
    assert by_id["cloud_sync"].status == StageExecutionStatus.SKIPPED
    assert by_id["notification"].status == StageExecutionStatus.SKIPPED
    mock_sync_cls.assert_not_called()
    mock_notify.assert_not_called()
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")


# ---------------------------------------------------------------------------
# A4 guard negative control (separated from regression assertions above)
# ---------------------------------------------------------------------------


def test_guard_trips_on_unstubbed_external() -> None:
    """Negative control: guard records the entry even though code absorbs the error."""
    from src.orchestration.master import _run_processing

    tripwire: list[str] = []
    with install_guard(tripwire):
        result = _run_processing({"date": "20260401"})
    assert tripwire != []
    assert tripwire[0] == "src.db.engine.get_db_session"
    assert result.status == StageExecutionStatus.FAILED


# ---------------------------------------------------------------------------
# B bulk_load placeholder false-success removal
# ---------------------------------------------------------------------------


def _execute_bulk(ctx: dict[str, Any], monkeypatch, mock_loader_cls: MagicMock) -> Any:
    """Execute bulk_load DAG under guard; fail on any tripwire hit outside the loader."""
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    tripwire: list[str] = []
    orch = MasterWorkflowOrchestrator.build_bulk_load_workflow()
    with ExitStack() as stack:
        stack.enter_context(install_guard(tripwire))
        stack.enter_context(patch("src.services.bulk_loader.BulkChunkLoader", new=mock_loader_cls))
        report = orch.execute_workflow("batch2_bulk", context=ctx)
    assert tripwire == [], f"test isolation breach: {tripwire}"
    return report


def _ok_loader_double(total_records: int = 5) -> MagicMock:
    """Loader double returning explicitly real-typed values (no MagicMock leakage)."""
    manifest = MagicMock()
    manifest.failed_partitions = 0
    manifest.total_records_processed = int(total_records)
    manifest.to_dict.return_value = {"ok": True, "total": int(total_records)}
    loader_cls = MagicMock()
    loader_cls.return_value.run_bulk_load.return_value = manifest
    return loader_cls


def _bulk_ctx() -> dict[str, Any]:
    return {"category": "PBP", "start_year": 2023, "end_year": 2024, "concurrency": 2}


def test_bulk_unimplemented_stages_reject_before_loading(monkeypatch) -> None:
    """Required unimplemented bulk stages reject before any real loading call."""
    mock_loader_cls = _ok_loader_double()
    report = _execute_bulk(_bulk_ctx(), monkeypatch, mock_loader_cls)
    by_id = {r.stage_id: r for r in report.stage_results}
    assert by_id["bulk_manifest"].status == StageExecutionStatus.FAILED
    assert by_id["bulk_manifest"].records_processed == 0
    for dep in ("bulk_ingest", "bulk_audit", "bulk_sync"):
        assert by_id[dep].status == StageExecutionStatus.SKIPPED, dep
        assert by_id[dep].records_processed == 0
    assert mock_loader_cls.call_count == 0
    assert report.overall_status in ("FAILED", "PARTIAL_FAILURE")


def test_bulk_placeholder_direct_calls_never_report_success() -> None:
    """Bypass path: direct placeholder invocation must not return fake success."""
    orch = MasterWorkflowOrchestrator.build_bulk_load_workflow()
    for stage_id in ("bulk_manifest", "bulk_audit", "bulk_sync"):
        result = orch._handlers[stage_id]({})
        assert result.status == StageExecutionStatus.FAILED, stage_id
        assert result.records_processed == 0, stage_id


def test_bulk_cli_non_dry_run_exits_nonzero(monkeypatch, capsys) -> None:
    """Bulk workflow failure propagates to exit code, JSON report, reason, and 0 loader calls."""
    import json

    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    mock_loader_cls = _ok_loader_double()
    tripwire: list[str] = []
    with ExitStack() as stack:
        stack.enter_context(install_guard(tripwire))
        stack.enter_context(patch("src.services.bulk_loader.BulkChunkLoader", new=mock_loader_cls))
        from src.cli.run_workflow import main as wf_main

        exit_code = wf_main(["--workflow", "bulk_load", "--json"])
    captured = capsys.readouterr()
    assert tripwire == [], f"test isolation breach: {tripwire}"
    assert mock_loader_cls.call_count == 0
    assert exit_code != 0
    # Extract JSON from output (there may be log lines before it)
    out = captured.out
    start = out.find("{")
    end = out.rfind("}") + 1
    if start == -1 or end == 0:
        msg = f"No JSON found in output: {out}"
        raise ValueError(msg)
    json_str = out[start:end]
    data = json.loads(json_str)
    assert data["overall_status"] in ("FAILED", "PARTIAL_FAILURE")
    assert data["failed_stages"] >= 1
    manifest_stage = next(s for s in data["stage_results"] if s["stage_id"] == "bulk_manifest")
    assert manifest_stage["status"] == "FAILED"
    assert manifest_stage["records_processed"] == 0
    assert "not implemented" in (manifest_stage["error_message"] or "")


def test_bulk_dry_run_executes_plan_without_real_work(monkeypatch, capsys) -> None:
    """Dry-run reveals plan execution only: SUCCESS with 0 loader calls under tripwire."""
    import json

    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    tripwire: list[str] = []
    with install_guard(tripwire):
        from src.cli.run_workflow import main as wf_main

        exit_code = wf_main(["--workflow", "bulk_load", "--dry-run", "--json"])
    captured = capsys.readouterr()
    # Loader entry stays guarded (no benign override): any real call would trip.
    assert tripwire == [], f"test isolation breach: {tripwire}"
    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["overall_status"] == "SUCCESS"
    assert data["total_stages"] == 4
    assert all(s["records_processed"] == 0 for s in data["stage_results"])


def test_bulk_ingest_implemented_path_preserved(monkeypatch) -> None:
    """The implemented ingest handler keeps its success/failure contract."""
    monkeypatch.setenv("WORKFLOW_SIMULATE_STAGES", "0")
    orch = MasterWorkflowOrchestrator.build_bulk_load_workflow()
    handler = orch._handlers["bulk_ingest"]

    ok_manifest = MagicMock(failed_partitions=0, total_records_processed=42)
    ok_loader = MagicMock()
    ok_loader.return_value.run_bulk_load.return_value = ok_manifest
    with patch("src.services.bulk_loader.BulkChunkLoader", new=ok_loader):
        ok_result = handler(_bulk_ctx())
    assert ok_result.status == StageExecutionStatus.COMPLETED
    assert ok_result.records_processed == 42
    ok_loader.assert_called_once_with(concurrency=2)

    bad_manifest = MagicMock(failed_partitions=2, total_records_processed=10)
    bad_loader = MagicMock()
    bad_loader.return_value.run_bulk_load.return_value = bad_manifest
    with patch("src.services.bulk_loader.BulkChunkLoader", new=bad_loader):
        bad_result = handler(_bulk_ctx())
    assert bad_result.status == StageExecutionStatus.FAILED
