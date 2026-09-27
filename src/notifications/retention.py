"""Retention policies for the notification stores.

Delivery audit rows and recovered incidents are append-only and would grow
without bound, so a scheduled job prunes them by age. The two windows are
independent (deliveries are kept longer than incidents) because a delivery is an
external side effect worth retaining even after the incident it announced has
been closed.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import delete

from src.models.notification_delivery import NotificationDelivery
from src.notifications.incident import IncidentManager

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

logger = logging.getLogger("src.notifications.retention")


def prune_delivery_history(session: Session, *, before: datetime) -> int:
    """Delete delivery audit rows dispatched before ``before``; return the count."""
    result = session.execute(
        delete(NotificationDelivery).where(NotificationDelivery.dispatched_at < before),
    )
    return int(result.rowcount or 0)


def prune_incident_history(session: Session, *, before: datetime) -> int:
    """Delete recovered incidents resolved before ``before``; return the count."""
    return IncidentManager(session).prune(recovered_before=before)
