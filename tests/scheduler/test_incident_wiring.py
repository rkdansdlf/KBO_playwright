"""Tests for wiring existing check results into the incident ledger (C6-C9)."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.base import Base
from src.models.notification_incident import NotificationIncident
from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.notifications.bridge import alerts_dry_run, apply_incidents
from src.orchestration.dto import StageExecutionStatus
from src.scheduler.jobs.daily import _failed_stage_ids, _publish_quality_incident
from src.scheduler.jobs.maintenance import _drift_alert_severity, _integrity_alert_event
from src.db.drift_dto import DriftSeverity


def _stage(stage_id: str, status: StageExecutionStatus, artifacts: dict | None = None, error: str | None = None):
    return SimpleNamespace(stage_id=stage_id, status=status, artifacts=artifacts or {}, error_message=error)


def _workflow_report(stages: list) -> SimpleNamespace:
    return SimpleNamespace(stage_results=stages)


class TestIntegrityEvent:
    def test_builds_error_event(self) -> None:
        result = SimpleNamespace(name="check_scores_populated", message="3 games missing scores")
        event = _integrity_alert_event(result, "20260925", "integrity:check_scores_populated:20260925")

        assert event.source == AlertSource.INTEGRITY
        assert event.component == "check_scores_populated"
        assert event.severity == AlertSeverity.ERROR
        assert event.incident_key == "integrity:check_scores_populated:20260925"
        assert "3 games missing scores" in event.message
        assert event.metadata["target_date"] == "20260925"


class TestQualityIncident:
    def test_pass_resolves(self) -> None:
        quality_report = SimpleNamespace(overall_status="PASS", quality_score=100, remediation_hints=[])
        stage = _stage(
            "quality_gate",
            StageExecutionStatus.COMPLETED,
            {"quality_report": quality_report},
        )

        with patch("src.notifications.bridge.apply_incidents") as mock:
            _publish_quality_incident(_workflow_report([stage]))

        assert mock.call_args.args[0] == []
        assert mock.call_args.kwargs["resolve_keys"] == ["quality:daily"]

    def test_warn_is_warning(self) -> None:
        quality_report = SimpleNamespace(overall_status="WARN", quality_score=88, remediation_hints=["fix it"])
        stage = _stage("quality_gate", StageExecutionStatus.COMPLETED, {"quality_report": quality_report})

        with patch("src.notifications.bridge.apply_incidents") as mock:
            _publish_quality_incident(_workflow_report([stage]))

        event = mock.call_args.args[0][0]
        assert event.incident_key == "quality:daily"
        assert event.severity == AlertSeverity.WARNING
        assert "88" in event.message
        assert event.remediation == ("fix it",)

    def test_fail_is_error(self) -> None:
        quality_report = SimpleNamespace(overall_status="FAIL", quality_score=40, remediation_hints=[])
        stage = _stage("quality_gate", StageExecutionStatus.FAILED, {"quality_report": quality_report})

        with patch("src.notifications.bridge.apply_incidents") as mock:
            _publish_quality_incident(_workflow_report([stage]))

        assert mock.call_args.args[0][0].severity == AlertSeverity.ERROR

    def test_missing_report_emits_error(self) -> None:
        stage = _stage("quality_gate", StageExecutionStatus.FAILED, {}, error="audit crashed")

        with patch("src.notifications.bridge.apply_incidents") as mock:
            _publish_quality_incident(_workflow_report([stage]))

        event = mock.call_args.args[0][0]
        assert event.severity == AlertSeverity.ERROR
        assert "audit crashed" in event.message

    def test_absent_quality_stage_is_noop(self) -> None:
        with patch("src.notifications.bridge.apply_incidents") as mock:
            _publish_quality_incident(_workflow_report([]))
        mock.assert_not_called()


class TestFailedStageIds:
    def test_collects_failed_stage_ids(self) -> None:
        report = _workflow_report(
            [
                _stage("ingestion", StageExecutionStatus.COMPLETED),
                _stage("quality_gate", StageExecutionStatus.FAILED),
            ],
        )
        assert _failed_stage_ids(report) == {"quality_gate"}

    def test_no_failures(self) -> None:
        report = _workflow_report([_stage("ingestion", StageExecutionStatus.COMPLETED)])
        assert _failed_stage_ids(report) == set()


class TestApplyIncidents:
    @pytest.fixture
    def session_factory(self):
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(bind=engine)
        return sessionmaker(bind=engine, expire_on_commit=False)

    def test_publishes_events_and_resolves_keys(self, session_factory) -> None:
        session = session_factory()
        session.add(
            NotificationIncident(
                incident_key="priority:test",
                source=AlertSource.QUALITY.value,
                component="test",
                severity=AlertSeverity.ERROR.value,
                state="OPEN",
                title="old",
                message="old",
                details_hash="x",
                occurrence_count=1,
                notification_count=1,
                first_opened_at=datetime(2026, 9, 25, 0, 0),
                last_seen_at=datetime(2026, 9, 25, 0, 0),
            ),
        )
        session.commit()
        session.close()

        event = AlertEvent(
            source=AlertSource.INTEGRITY,
            component="check_games_exist",
            severity=AlertSeverity.ERROR,
            title="integrity",
            message="missing games",
            incident_key="integrity:check_games_exist:20260925",
        )
        apply_incidents([event], resolve_keys=["priority:test"], session_factory=session_factory)

        with session_factory() as session:
            rows = {r.incident_key: r for r in session.execute(select(NotificationIncident)).scalars().all()}
        assert "integrity:check_games_exist:20260925" in rows
        assert rows["priority:test"].state == "RECOVERED"

    def test_empty_input_is_noop(self, session_factory) -> None:
        apply_incidents([], resolve_keys=[], session_factory=session_factory)

    def test_errors_are_contained(self) -> None:
        def broken_factory():
            raise RuntimeError("db down")

        event = AlertEvent(
            source=AlertSource.INTEGRITY,
            component="x",
            severity=AlertSeverity.ERROR,
            title="t",
            message="m",
            incident_key="integrity:x",
        )
        apply_incidents([event], session_factory=broken_factory)


class TestDryRunFlag:
    def test_dry_run_reads_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALERT_DRY_RUN", "1")
        assert alerts_dry_run() is True
        monkeypatch.setenv("ALERT_DRY_RUN", "0")
        assert alerts_dry_run() is False
        monkeypatch.delenv("ALERT_DRY_RUN", raising=False)
        assert alerts_dry_run() is False


class TestDriftSeverity:
    def test_high_maps_to_error(self) -> None:
        drifts = [SimpleNamespace(severity=DriftSeverity.HIGH)]
        assert _drift_alert_severity(drifts) == AlertSeverity.ERROR

    def test_medium_maps_to_warning(self) -> None:
        drifts = [SimpleNamespace(severity=DriftSeverity.MEDIUM)]
        assert _drift_alert_severity(drifts) == AlertSeverity.WARNING

    def test_low_maps_to_info(self) -> None:
        drifts = [SimpleNamespace(severity=DriftSeverity.LOW)]
        assert _drift_alert_severity(drifts) == AlertSeverity.INFO

    def test_worst_severity_wins(self) -> None:
        drifts = [SimpleNamespace(severity=DriftSeverity.LOW), SimpleNamespace(severity=DriftSeverity.HIGH)]
        assert _drift_alert_severity(drifts) == AlertSeverity.ERROR


class TestSchemaDriftJob:
    def test_no_drift_resolves_incident(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.scheduler.jobs import maintenance

        report = SimpleNamespace(drift_count=0, total_tables_checked=90, drifts=[], generated_ddl=[], dialect="oracle")
        monkeypatch.setattr(
            "src.db.drift_detector.SchemaDriftDetector", lambda _engine: SimpleNamespace(detect_drift=lambda: report)
        )
        monkeypatch.setattr("src.scheduler.jobs.maintenance._scheduler_job_lock", _null_lock)

        with patch("src.notifications.bridge.apply_incidents") as mock:
            maintenance.schema_drift_check_job()

        assert mock.call_args.args[0] == []
        assert mock.call_args.kwargs["resolve_keys"] == ["drift:schema"]

    def test_drift_publishes_incident(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.scheduler.jobs import maintenance

        drift = SimpleNamespace(drift_type=SimpleNamespace(value="MISSING_COLUMN"), severity=DriftSeverity.HIGH)
        report = SimpleNamespace(
            drift_count=1,
            total_tables_checked=90,
            drifts=[drift],
            generated_ddl=["ALTER TABLE t ADD c INT"],
            dialect="oracle",
        )
        monkeypatch.setattr(
            "src.db.drift_detector.SchemaDriftDetector", lambda _engine: SimpleNamespace(detect_drift=lambda: report)
        )
        monkeypatch.setattr("src.scheduler.jobs.maintenance._scheduler_job_lock", _null_lock)

        with patch("src.notifications.bridge.apply_incidents") as mock:
            maintenance.schema_drift_check_job()

        event = mock.call_args.args[0][0]
        assert event.incident_key == "drift:schema"
        assert event.severity == AlertSeverity.ERROR
        assert event.remediation == ("ALTER TABLE t ADD c INT",)


class _null_lock:
    """Context manager that acquires nothing (test double for scheduler locks)."""

    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def __enter__(self) -> _null_lock:
        return self

    def __exit__(self, *_exc) -> bool:
        return False


class TestPregameIncident:
    DATE = "20260925"
    KEY = "freshness:pregame:20260925"

    def test_missing_publishes_warning_incident(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.scheduler.jobs import live

        summaries = iter([(1, 2, 1), (1, 2, 1)])

        async def _fake_batch(_target_date: str) -> list[str]:
            return ["saved"]

        monkeypatch.setattr(live, "_pregame_refresh_summary", lambda _d: next(summaries))
        monkeypatch.setattr(live, "run_preview_batch", _fake_batch)

        with patch("src.notifications.bridge.apply_incidents") as apply:
            live._process_pregame_date(self.DATE, refresh_only_missing=True, alert_on_missing=True)

        event = apply.call_args.args[0][0]
        assert event.incident_key == self.KEY
        assert event.severity == AlertSeverity.WARNING
        assert event.metadata["starters_missing"] == 2

    def test_covered_resolves_incident(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.scheduler.jobs import live

        monkeypatch.setattr(live, "_pregame_refresh_summary", lambda _d: (1, 0, 0))

        with patch("src.notifications.bridge.apply_incidents") as apply:
            live._process_pregame_date(self.DATE, refresh_only_missing=True, alert_on_missing=True)

        assert apply.call_args.args[0] == []
        assert apply.call_args.kwargs["resolve_keys"] == [self.KEY]

    def test_alert_disabled_does_not_publish(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.scheduler.jobs import live

        summaries = iter([(1, 2, 1), (1, 2, 1)])

        async def _fake_batch(_target_date: str) -> list[str]:
            return ["saved"]

        monkeypatch.setattr(live, "_pregame_refresh_summary", lambda _d: next(summaries))
        monkeypatch.setattr(live, "run_preview_batch", _fake_batch)

        with patch("src.notifications.bridge.apply_incidents") as apply:
            live._process_pregame_date(self.DATE, refresh_only_missing=True, alert_on_missing=False)

        # With alerting disabled, no incident is opened and nothing is resolved.
        apply.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
