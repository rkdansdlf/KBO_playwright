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

    # The PARTIAL_FAILURE branch calls alert_warning. Left real, that dispatches
    # for real and DeliveryRecorder commits a notification_deliveries row -- a
    # unit test writing to the ledger, and a write that waits out SQLite's
    # busy_timeout (120s, src/db/engine.py) whenever anything else holds the file.
    mock_alert = MagicMock()
    monkeypatch.setattr("src.scheduler.jobs.daily.alert_warning", mock_alert)
    monkeypatch.setattr("src.scheduler.alerting.alert_warning", mock_alert)
    if "src.scheduler" in sys.modules:
        monkeypatch.setattr(sys.modules["src.scheduler"], "alert_warning", mock_alert, raising=False)
    if "scripts.scheduler" in sys.modules:
        monkeypatch.setattr(sys.modules["scripts.scheduler"], "alert_warning", mock_alert, raising=False)

    clear_job_registry()
    try:
        crawl_daily_games()
        assert _JOB_REGISTRY["crawl_daily_games"].status is JobStatus.FAILURE
        mock_alert.assert_called_once()
    finally:
        clear_job_registry()


class TestTheDailyQualityIncidentNamesWhatChanged:
    """The incident is the only place this report reaches an operator.

    A score cannot distinguish a gate that has been sitting on the same backlog
    for a month from one that gained a damaged player overnight, and those season
    violations cannot be repaired from here. The counts are what makes the
    message actionable, so they are asserted rather than left to a format string.
    """

    @staticmethod
    def _publish(report) -> object:
        # The publisher imports ``apply_incidents`` inside its own body, so the
        # module has to be patched before the function is bound or the patch
        # lands after the name it replaces was already taken.
        with patch("src.notifications.bridge.apply_incidents") as applied:
            from src.scheduler.jobs.daily import _publish_quality_incident

            _publish_quality_incident(report)
        return applied.call_args

    @staticmethod
    def _report(pack):
        """Wrap the audit the way the DAG does.

        ``_publish_quality_incident`` reads ``stage_results`` and digs the audit
        out of the stage's ``artifacts``, so handing it the audit directly would
        return before reaching any of this.
        """
        from src.services.quality_hub import UnifiedQualityReport

        audit = UnifiedQualityReport(
            timestamp="2026-10-12T03:00:00+09:00",
            overall_status="WARN",
            quality_score=79,
            regression_pack=pack,
        )
        return _stage_report(stage_id="quality_gate", artifacts={"quality_report": audit})

    def test_the_message_separates_new_damage_from_the_backlog(self) -> None:
        from src.services.quality_hub import RegressionPackSummary

        report = self._report(
            RegressionPackSummary(
                ok=False,
                check_count=13,
                failure_count=3,
                baseline_available=True,
                new_violation_count=46,
                baseline_violation_count=760,
            ),
        )

        event = self._publish(report).args[0][0]

        assert "79/100" in event.message
        assert "신규 46" in event.message
        assert "백로그 760" in event.message
        assert event.metadata["new_violation_count"] == 46
        assert event.metadata["baseline_violation_count"] == 760

    def test_no_breakdown_is_invented_without_a_baseline(self) -> None:
        """With nothing to compare against, counts would read as measurements."""
        from src.services.quality_hub import RegressionPackSummary

        report = self._report(RegressionPackSummary(ok=False, check_count=13, failure_count=3))

        event = self._publish(report).args[0][0]

        assert event.message == "quality score 79/100"
        assert "신규" not in event.message
        assert "baseline_available" not in event.metadata

    def test_a_row_that_can_never_be_recovered_is_named_separately(self) -> None:
        """It is not new damage and not a backlog item; it is a third thing."""
        from src.services.quality_hub import RegressionPackSummary

        report = self._report(
            RegressionPackSummary(
                ok=False,
                check_count=13,
                failure_count=3,
                baseline_available=True,
                new_violation_count=0,
                baseline_violation_count=760,
                newly_unrecoverable_count=20,
            ),
        )

        event = self._publish(report).args[0][0]

        assert "복구불가 20" in event.message
        assert event.metadata["newly_unrecoverable_count"] == 20


def _stage_report(*, stage_id: str, artifacts: dict, error_message: str | None = None) -> MagicMock:
    """A DAG report carrying one stage, with the attributes the incident reads.

    A bare MagicMock answers any attribute, so ``artifacts`` has to be a real dict
    for ``or {}`` and ``.get`` in the publisher to mean what they read as.
    """
    stage = MagicMock()
    stage.stage_id = stage_id
    stage.artifacts = artifacts
    stage.error_message = error_message
    report = MagicMock()
    report.stage_results = [stage]
    return report


class TestAPassWithNoBaselineStillReachesTheOperator:
    """A passing gate is the least likely moment to be believed about anything.

    ``_publish_quality_incident`` resolves the incident and returns the moment the
    status is PASS, so a run that has never been measured looks identical to a run
    that was measured and found nothing wrong. The baseline comparison is the part
    of the report that distinguishes those two, and on a clean database it is the
    only thing left to say.

    So a passing run that was asked to compare and could not is opened as a
    notice rather than swallowed. The gate has no verdict to report, and staying
    silent would mean the comparison is never switched on by anyone.
    """

    @staticmethod
    def _publish(report) -> object:
        # The publisher imports ``apply_incidents`` inside its own body, so the
        # module has to be patched before the function is bound or the patch
        # lands after the name it replaces was already taken.
        with patch("src.notifications.bridge.apply_incidents") as applied:
            from src.scheduler.jobs.daily import _publish_quality_incident

            _publish_quality_incident(report)
        return applied.call_args

    @staticmethod
    def _report(pack, hints: tuple[str, ...] = ()):
        from src.services.quality_hub import UnifiedQualityReport

        return _stage_report(
            stage_id="quality_gate",
            artifacts={
                "quality_report": UnifiedQualityReport(
                    timestamp="2026-10-12T03:00:00+09:00",
                    overall_status="PASS",
                    quality_score=100,
                    regression_pack=pack,
                    remediation_hints=list(hints),
                ),
            },
        )

    def test_a_pass_without_a_comparison_opens_a_notice_instead_of_resolving(self) -> None:
        from src.services.quality_hub import RegressionPackSummary

        args = self._publish(
            self._report(
                RegressionPackSummary(ok=True, check_count=13, failure_count=0),
                hints=("python3 -m src.cli.data_quality_regression_pack --write-baseline --season 2026",),
            ),
        )

        assert args is not None, "a passing run issued no incident at all"
        events = args.args[0]
        assert events, "a passing run resolved the incident instead of reporting the missing baseline"
        event = events[0]
        assert event.incident_key == "quality:daily"
        assert "baseline" in " ".join(event.remediation).lower()

    def test_a_pass_with_a_comparison_still_resolves(self) -> None:
        """Once the baseline exists there is nothing left to report, and silence is right."""
        from src.services.quality_hub import RegressionPackSummary

        args = self._publish(
            self._report(
                RegressionPackSummary(
                    ok=True,
                    check_count=13,
                    failure_count=0,
                    baseline_available=True,
                    baseline_violation_count=0,
                ),
            ),
        )

        assert args.args[0] == []
        assert args.kwargs.get("resolve_keys") == ["quality:daily"]

    def test_a_stage_that_produced_no_audit_is_a_failure_not_a_pass(self) -> None:
        """The other silent case: the stage ran and produced nothing to judge."""
        report = _stage_report(stage_id="quality_gate", artifacts={}, error_message="no audit produced")

        args = self._publish(report)

        events = args.args[0]
        assert events, "a stage that produced no audit was reported as a pass"
        assert events[0].severity.name == "ERROR"
        assert "no audit produced" in events[0].message


class TestTheMissingBaselineNoticeCarriesItsInstruction:
    """An alarm with nothing to act on is worse than silence.

    The notice is opened because the gate could not compare, but the reader's
    next step is in ``remediation_hints``. A run that was asked to compare and
    produced no hint has failed somewhere else, and opening an incident naming
    "no baseline" would send the operator to a database that is fine.
    """

    @staticmethod
    def _publish(report) -> object:
        with patch("src.notifications.bridge.apply_incidents") as applied:
            from src.scheduler.jobs.daily import _publish_quality_incident

            _publish_quality_incident(report)
        return applied.call_args

    def test_a_pass_with_no_comparison_and_no_instruction_stays_silent(self) -> None:
        from src.services.quality_hub import RegressionPackSummary, UnifiedQualityReport

        report = _stage_report(
            stage_id="quality_gate",
            artifacts={
                "quality_report": UnifiedQualityReport(
                    timestamp="2026-10-12T03:00:00+09:00",
                    overall_status="PASS",
                    quality_score=100,
                    regression_pack=RegressionPackSummary(ok=True, check_count=13, failure_count=0),
                    remediation_hints=[],
                ),
            },
        )

        args = self._publish(report)

        assert args.args[0] == [], "an incident opened with nothing to tell the reader to do"
        assert args.kwargs.get("resolve_keys") == ["quality:daily"]

    def test_the_notice_names_the_record_command(self) -> None:
        from src.services.quality_hub import RegressionPackSummary, UnifiedQualityReport

        report = _stage_report(
            stage_id="quality_gate",
            artifacts={
                "quality_report": UnifiedQualityReport(
                    timestamp="2026-10-12T03:00:00+09:00",
                    overall_status="PASS",
                    quality_score=100,
                    regression_pack=RegressionPackSummary(ok=True, check_count=13, failure_count=0),
                    remediation_hints=[
                        "python3 -m src.cli.data_quality_regression_pack --write-baseline --season 2026"
                    ],
                ),
            },
        )

        event = self._publish(report).args[0][0]

        assert event.severity.name == "WARNING"
        assert any("--write-baseline" in hint for hint in event.remediation)
        assert "기준선" in event.message
