from __future__ import annotations

from pathlib import Path

import pytest

from src.notifications.recorder import DeliveryRecorder


class _UnexpectedDeliveryAuditWrite(AssertionError):
    """A scheduler unit test reached the delivery audit recorder."""


@pytest.fixture(autouse=True)
def _job_registry_writes_go_to_a_temp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep `_persist_job_registry` out of the project's runtime file.

    ``_update_job_status`` persists the whole registry as a side effect, so any
    scheduler test that updates a job status rewrites
    ``data/last_runs/job_registry.json``. That file is what a human reads to
    learn whether the pipeline ran, and a test fixture's mocked report is
    indistinguishable in it from a real run: on 2026-10-12 it held
    ``{"test_job": {"status": "success", "message": "Done"}}``, and reading it
    as evidence produced a fabricated investigation into two "DAG runs" and an
    incident that had never been updated.

    Redirecting keeps the tests honest -- they still exercise persistence and
    restart recovery -- while the runtime file stops being a test output.
    ``JOB_REGISTRY_FILE`` is the override the production code already reads, so
    no production path changes to accommodate the tests.
    """
    monkeypatch.setenv("JOB_REGISTRY_FILE", str(tmp_path / "job_registry.json"))


@pytest.fixture(autouse=True)
def _no_real_delivery_audit_write_outside_incident_tests(request, monkeypatch):
    """Fail scheduler unit tests that persist a delivery audit row.

    Scheduler tests should assert on the busines/intent seam, not write to the
    application database. A unit test cannot know what else holds SQLite's busy
    timeout, so the row write becomes 120 seconds and no log says why. The only
    scheduler test module allowed to keep the real write is the incident-ledger
    contract test, which uses an isolated session factory.
    """
    if request.path.name == "test_incident_wiring.py":
        return

    def refuse_real_record(self, report, *, incident_id=None, notification_type="notification", recorded_at=None):
        from src.models.notification_delivery import DELIVERY_STATUS_SUPPRESSED

        if not [r for r in getattr(report, "results", []) if getattr(r, "status", None) != DELIVERY_STATUS_SUPPRESSED]:
            return 0
        raise _UnexpectedDeliveryAuditWrite(
            "DeliveryRecorder.record reached the database from a scheduler unit test. "
            "Stub the alert bridge (alert_warning/_publish_quality_incident/apply_incidents); "
            "real incident-ledger writes belong in tests/notifications or test_incident_wiring.py."
        )

    monkeypatch.setattr(DeliveryRecorder, "record", refuse_real_record)
