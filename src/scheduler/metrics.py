"""Job lifecycle listener and metrics reporting for the KBO scheduler.

The listener records Prometheus metrics and captures exceptions in Sentry. Job
failures are published as incidents (``scheduler:<job>:failed``) so that dedup,
cooldown and RECOVERED delivery are owned by the incident pipeline rather than a
bespoke in-memory throttle.
"""

from __future__ import annotations

import logging
import time

import sentry_sdk
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_SUBMITTED

from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.notifications.bridge import apply_incidents
from src.utils.metrics import (
    KBO_SCHEDULER_JOB_DURATION_SECONDS,
    KBO_SCHEDULER_JOB_TOTAL,
)

logger = logging.getLogger("src.scheduler.metrics")

job_start_times: dict[str, float] = {}

_TRACEBACK_LIMIT = 2000


def _scheduler_failure_key(job_id: str) -> str:
    return f"scheduler:{job_id}:failed"


def job_lifecycle_listener(event: object) -> None:
    """Listen for APScheduler job lifecycle events to collect metrics and capture errors."""
    event_code = getattr(event, "code", None)
    job_id = getattr(event, "job_id", "unknown")

    if event_code == EVENT_JOB_SUBMITTED:
        job_start_times[job_id] = time.time()

    elif event_code == EVENT_JOB_EXECUTED:
        start_time = job_start_times.pop(job_id, None)
        duration = time.time() - start_time if start_time else 0.0

        KBO_SCHEDULER_JOB_TOTAL.labels(job_id=job_id, status="success").inc()
        KBO_SCHEDULER_JOB_DURATION_SECONDS.labels(job_id=job_id).observe(duration)
        apply_incidents([], resolve_keys=[_scheduler_failure_key(job_id)])

    elif event_code == EVENT_JOB_ERROR:
        _handle_job_error(event, job_id)


def _handle_job_error(event: object, job_id: str) -> None:
    """Record a failure, capture it in Sentry, and publish an incident."""
    start_time = job_start_times.pop(job_id, None)
    duration = time.time() - start_time if start_time else 0.0

    KBO_SCHEDULER_JOB_TOTAL.labels(job_id=job_id, status="failure").inc()
    KBO_SCHEDULER_JOB_DURATION_SECONDS.labels(job_id=job_id).observe(duration)

    exc = getattr(event, "exception", None)
    if not exc:
        return

    import traceback

    tb = "".join(traceback.format_exception(type(exc), exc, getattr(exc, "__traceback__", None)))
    logger.error("Job %s failed: %s", job_id, exc)
    sentry_sdk.capture_exception(exc)

    apply_incidents(
        [
            AlertEvent(
                source=AlertSource.SCHEDULER,
                component=job_id,
                severity=AlertSeverity.ERROR,
                title=f"스케줄러 잡 실패: {job_id}",
                message=f"{type(exc).__name__}: {exc}\n\n{tb[:_TRACEBACK_LIMIT]}",
                incident_key=_scheduler_failure_key(job_id),
                metadata={"job": job_id, "exception_type": type(exc).__name__},
            ),
        ],
    )
