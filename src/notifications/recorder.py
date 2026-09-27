"""Append-only delivery audit recorder.

Owns its **own** short transaction, separate from the caller's business
transaction. This is the whole point of the component:

* a successful Telegram send is a real external side effect, so the audit row
  must survive even if the caller's incident/business transaction later rolls
  back;
* a failed audit write must never turn a successful send into a notification
  failure, so any persistence error is contained, counted and logged.

The recorder consumes the already-built
:class:`~src.notifications.dto.NotificationBatchReport`, so a fan-out writes one
row per channel sharing a single ``batch_id``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy.exc import SQLAlchemyError

from src.models.notification_delivery import (
    DELIVERY_STATUS_SUPPRESSED,
    NotificationDelivery,
)
from src.notifications.alert_dto import utcnow
from src.utils.metrics import (
    record_notification_delivery_audit_failure,
    record_notification_delivery_persisted,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from src.notifications.dto import NotificationBatchReport

logger = logging.getLogger("src.notifications.recorder")

#: Failures a delivery audit write may raise. They are swallowed so the transport
#: result is never contaminated by an audit-persistence problem.
AUDIT_EXCEPTIONS = (
    SQLAlchemyError,
    OSError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    ImportError,
)

#: Seconds to milliseconds.
_MS = 1000.0


def default_session_factory() -> object:
    """Return a new application session without importing the DB engine eagerly."""
    from src.db.engine import SessionLocal

    return SessionLocal()


class DeliveryRecorder:
    """Persist delivery audit rows in an independent transaction."""

    def __init__(self, session_factory: Callable[[], object]) -> None:
        """Initialize the recorder with a session factory (never a live session)."""
        self._session_factory = session_factory

    def record(
        self,
        report: NotificationBatchReport,
        *,
        incident_id: int | None = None,
        notification_type: str = "notification",
        recorded_at: datetime | None = None,
    ) -> int:
        """Persist one row per channel result; return the number of rows written.

        ``SUPPRESSED`` results never entered the dispatch pipeline and are not
        delivery rows, so they are skipped.
        """
        results = [r for r in report.results if r.status != DELIVERY_STATUS_SUPPRESSED]
        if not results:
            return 0

        moment = recorded_at or utcnow()
        batch_id = uuid4().hex
        try:
            with self._session_factory() as session:
                for result in results:
                    latency = result.duration_seconds or 0.0
                    completed = moment
                    session.add(
                        NotificationDelivery(
                            incident_id=incident_id,
                            notification_type=notification_type,
                            batch_id=batch_id,
                            channel=result.channel.value,
                            destination=result.destination,
                            status=result.status,
                            attempt_count=max(1, result.attempt_count),
                            dispatched_at=completed,
                            completed_at=completed,
                            latency_ms=round(latency * _MS),
                            error_code=None,
                            error_message=result.error_message,
                        ),
                    )
                session.commit()
        except AUDIT_EXCEPTIONS:
            record_notification_delivery_audit_failure()
            logger.exception(
                "Delivery audit write failed for batch %s; transport result is preserved",
                batch_id,
            )
            return 0

        for result in results:
            record_notification_delivery_persisted(result.channel.value, attempt_count=result.attempt_count)
        return len(results)
