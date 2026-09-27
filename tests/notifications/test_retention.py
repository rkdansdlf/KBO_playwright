"""Retention policy tests for delivery and incident history."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.notification_delivery import NotificationDelivery
from src.models.notification_incident import NotificationIncident
from src.notifications.retention import prune_delivery_history, prune_incident_history

NOW = datetime(2026, 9, 25, 6, 0, 0)


@pytest.fixture
def session_factory(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'retention.db'}")
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _delivery(dispatched_at: datetime) -> NotificationDelivery:
    return NotificationDelivery(
        notification_type="alert",
        batch_id="b",
        channel="telegram",
        status="SENT",
        attempt_count=1,
        dispatched_at=dispatched_at,
    )


def _incident(key: str, state: str, resolved_at: datetime | None) -> NotificationIncident:
    return NotificationIncident(
        incident_key=key,
        source="quality",
        component="daily",
        severity="ERROR",
        state=state,
        title="t",
        message="m",
        details_hash="x",
        occurrence_count=1,
        notification_count=1,
        first_opened_at=NOW - timedelta(days=60),
        last_seen_at=NOW,
        resolved_at=resolved_at,
    )


class TestDeliveryRetention:
    def test_prunes_only_rows_before_cutoff(self, session_factory) -> None:
        with session_factory() as session:
            session.add(_delivery(NOW - timedelta(days=120)))
            session.add(_delivery(NOW - timedelta(days=10)))
            session.commit()

            removed = prune_delivery_history(session, before=NOW - timedelta(days=90))
            session.commit()

            remaining = list(session.query(NotificationDelivery).all())

        assert removed == 1
        assert len(remaining) == 1

    def test_no_rows_before_cutoff_is_noop(self, session_factory) -> None:
        with session_factory() as session:
            session.add(_delivery(NOW - timedelta(days=1)))
            session.commit()
            assert prune_delivery_history(session, before=NOW - timedelta(days=90)) == 0


class TestIncidentRetention:
    def test_prunes_only_recovered_incidents_before_cutoff(self, session_factory) -> None:
        with session_factory() as session:
            session.add(_incident("old-recovered", "RECOVERED", NOW - timedelta(days=40)))
            session.add(_incident("recent-recovered", "RECOVERED", NOW - timedelta(days=1)))
            session.add(_incident("still-open", "OPEN", None))
            session.commit()

            removed = prune_incident_history(session, before=NOW - timedelta(days=30))
            session.commit()

            remaining = {row.incident_key for row in session.query(NotificationIncident).all()}

        assert removed == 1
        assert remaining == {"recent-recovered", "still-open"}


class TestRetentionJob:
    def test_job_uses_env_windows(self, monkeypatch, tmp_path) -> None:
        from unittest.mock import MagicMock, patch

        from src.scheduler.jobs import maintenance

        engine = create_engine(f"sqlite:///{tmp_path / 'job.db'}")
        Base.metadata.create_all(bind=engine)
        maker = sessionmaker(bind=engine, expire_on_commit=False)

        monkeypatch.setenv("NOTIFICATION_DELIVERY_RETENTION_DAYS", "7")
        monkeypatch.setenv("NOTIFICATION_INCIDENT_RETENTION_DAYS", "3")
        monkeypatch.setattr(maintenance, "SessionLocal", maker)
        monkeypatch.setattr("src.scheduler.jobs.maintenance._scheduler_job_lock", _null_lock)

        with (
            patch("src.notifications.retention.prune_delivery_history", MagicMock(return_value=2)) as deliveries,
            patch("src.notifications.retention.prune_incident_history", MagicMock(return_value=1)) as incidents,
        ):
            maintenance.notification_retention_job()

        assert deliveries.call_args.kwargs["before"] is not None
        assert incidents.call_args.kwargs["before"] is not None


class _null_lock:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def __enter__(self) -> _null_lock:
        return self

    def __exit__(self, *_exc) -> bool:
        return False


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
