"""Alerting helper functions for scheduler tasks.

These helpers are a thin adapter over :func:`src.notifications.bridge.apply_incidents`.
A job that warns OPENS ``scheduler:<job>:warning``; a job that exhausts its retries
OPENS ``scheduler:<job>:failed``; a successful run RECOVERS both. Delivery,
deduplication and cooldown are owned by the incident pipeline, not by this module.
"""

from __future__ import annotations

import logging
import os

from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.notifications.bridge import apply_incidents

logger = logging.getLogger("src.scheduler.alerting")


def _success_notifications_enabled() -> bool:
    return os.getenv("NOTIFY_SUCCESS", "0") == "1"


def alert_failure(retry_state: object) -> None:
    """Open a scheduler-failure incident after a job exhausts its retries."""
    fn_name = "unknown"
    fn = getattr(retry_state, "fn", None)
    if fn is not None:
        fn_name = getattr(fn, "__name__", "unknown")
    outcome = getattr(retry_state, "outcome", None)
    exc = outcome.exception() if outcome else "Unknown error"
    attempt = getattr(retry_state, "attempt_number", "?")
    logger.error("Job %s failed on attempt %s: %s", fn_name, attempt, exc)

    apply_incidents(
        [
            AlertEvent(
                source=AlertSource.SCHEDULER,
                component=fn_name,
                severity=AlertSeverity.ERROR,
                title=f"스케줄러 잡 영구 실패: {fn_name}",
                message=f"attempts={attempt}, error={exc}",
                incident_key=f"scheduler:{fn_name}:failed",
                metadata={"attempts": str(attempt), "job": fn_name},
            ),
        ],
    )


def alert_warning(func_name: str, details: str | None = None) -> None:
    """Open (or refresh) a scheduler-warning incident for a job."""
    logger.warning("Job %s emitted warning: %s", func_name, details)
    apply_incidents(
        [
            AlertEvent(
                source=AlertSource.SCHEDULER,
                component=func_name,
                severity=AlertSeverity.WARNING,
                title=f"스케줄러 잡 경고: {func_name}",
                message=details or "No details provided",
                incident_key=f"scheduler:{func_name}:warning",
                metadata={"job": func_name},
            ),
        ],
    )


def alert_success(func_name: str, details: str | None = None) -> None:
    """Recover any open incident for a job.

    Incident state is always resolved so a later failure is not mistaken for a
    recurrence of a recovered one. The RESOLVED notification is only delivered
    when ``NOTIFY_SUCCESS=1`` (``dry_run`` still updates state without sending).
    """
    logger.info("Job %s succeeded: %s", func_name, details)
    apply_incidents(
        [],
        resolve_keys=[f"scheduler:{func_name}:warning", f"scheduler:{func_name}:failed"],
        dry_run=not _success_notifications_enabled(),
    )
