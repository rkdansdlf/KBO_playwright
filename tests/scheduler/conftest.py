from __future__ import annotations

import pytest

from src.notifications.recorder import DeliveryRecorder


class _UnexpectedDeliveryAuditWrite(AssertionError):
    """A scheduler unit test reached the delivery audit recorder."""


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
