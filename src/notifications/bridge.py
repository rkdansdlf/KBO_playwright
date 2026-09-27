"""Shared helper for publishing incidents from existing check results.

Lives in :mod:`src.notifications` (not the scheduler) so both scheduled jobs and
CLI reporting commands can use it without an inverted dependency. Alert wiring
must never break the caller, so failures are contained and logged.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.notifications.publisher import AlertPublisher
from src.notifications.recorder import DeliveryRecorder

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from datetime import datetime

    from src.notifications.alert_dto import AlertEvent

logger = logging.getLogger("src.notifications.bridge")

#: Failures that alert wiring may raise; they are logged and swallowed so the
#: originating check result is never lost because alerting broke.
ALERT_WIRING_EXCEPTIONS = (
    SQLAlchemyError,
    OSError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    ImportError,
)


def alerts_dry_run() -> bool:
    """Return whether alert delivery is globally disabled via ``ALERT_DRY_RUN``."""
    return os.getenv("ALERT_DRY_RUN", "0").strip().lower() in ("1", "true", "yes", "on")


def apply_incidents(  # noqa: PLR0913 - keyword-only options; callers rely on names
    events: Iterable[AlertEvent],
    *,
    resolve_keys: Iterable[str] = (),
    reconcile_prefix: str | None = None,
    session_factory: Callable[[], object] | None = None,
    dry_run: bool | None = None,
    now: datetime | None = None,
) -> None:
    """Open/update incidents for ``events`` and recover incidents that have cleared.

    Two recovery modes are supported:

    * ``resolve_keys`` — explicit incident keys whose checks now pass.
    * ``reconcile_prefix`` — every active incident under a key namespace
      (for example ````scheduler:lock_skip:````) that is *not* in this batch is
      treated as recovered. Use this for recurring threshold checks where a
      passing run simply reports fewer keys.

    Args:
        events: Failure events to publish.
        resolve_keys: Incident keys whose checks now pass.
        reconcile_prefix: Optional key namespace to reconcile within.
        session_factory: Optional session factory override (tests).
        dry_run: Override the ``ALERT_DRY_RUN`` environment default.
        now: Optional single instant pinned for the whole batch.

    """
    events = list(events)
    resolve_keys = list(resolve_keys)
    if not events and not resolve_keys and not reconcile_prefix:
        return

    effective_dry_run = alerts_dry_run() if dry_run is None else dry_run
    factory = session_factory or SessionLocal
    try:
        with factory() as session:
            publisher = AlertPublisher(session, recorder=DeliveryRecorder(factory))
            for event in events:
                publisher.publish(event, now=now, dry_run=effective_dry_run)
            for key in resolve_keys:
                publisher.resolve(key, now=now, dry_run=effective_dry_run)
            if reconcile_prefix:
                published = {event.incident_key for event in events}
                for incident in publisher.manager.active_incidents():
                    if not incident.incident_key.startswith(reconcile_prefix):
                        continue
                    if incident.incident_key in published:
                        continue
                    publisher.resolve(incident.incident_key, now=now, dry_run=effective_dry_run)
            publisher.refresh_metrics()
            session.commit()
    except ALERT_WIRING_EXCEPTIONS:
        logger.exception("Incident wiring failed; caller result is unaffected")
