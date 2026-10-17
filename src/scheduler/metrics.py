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
from src.scheduler.locks import consume_gate_skip
from src.utils.metrics import (
    KBO_SCHEDULER_JOB_CONSECUTIVE_SKIPS,
    KBO_SCHEDULER_JOB_DURATION_SECONDS,
    KBO_SCHEDULER_JOB_LAST_RUN_TIMESTAMP,
    KBO_SCHEDULER_JOB_TOTAL,
)

logger = logging.getLogger("src.scheduler.metrics")

job_start_times: dict[str, float] = {}

_TRACEBACK_LIMIT = 2000


def _scheduler_failure_key(job_id: str) -> str:
    return f"scheduler:{job_id}:failed"


def _record_job_outcome(job_id: str) -> None:
    """Fold a finished run into the job's consecutive-skip streak.

    Read here rather than in the gate, because this is the only place that knows
    the *registered* job id: a gate sees the function's ``__name__``, and the two
    differ (``crawl_daily_games_job`` is registered as ``crawl_daily_games``).
    Recording under the function name would put the skip on a series that neither
    ``kbo_scheduler_job_total`` nor ``kbo_scheduler_job_last_run_timestamp``
    carries, and an operator could not join them.

    A run that executed -- success or failure -- resets the streak. A run that
    never reached the body extends it; only the gate can tell the two apart, and
    it says so through the thread-local flag.
    """
    gauge = KBO_SCHEDULER_JOB_CONSECUTIVE_SKIPS.labels(job_id=job_id)
    if consume_gate_skip():
        gauge.inc()
    else:
        gauge.set(0)


def job_lifecycle_listener(event: object) -> None:
    """Listen for APScheduler job lifecycle events to collect metrics and capture errors."""
    event_code = getattr(event, "code", None)
    job_id = getattr(event, "job_id", "unknown")

    if event_code == EVENT_JOB_SUBMITTED:
        job_start_times[job_id] = time.time()
        KBO_SCHEDULER_JOB_LAST_RUN_TIMESTAMP.labels(job_id=job_id).set(job_start_times[job_id])

    elif event_code == EVENT_JOB_EXECUTED:
        start_time = job_start_times.pop(job_id, None)
        duration = time.time() - start_time if start_time else 0.0

        KBO_SCHEDULER_JOB_TOTAL.labels(job_id=job_id, status="success").inc()
        KBO_SCHEDULER_JOB_DURATION_SECONDS.labels(job_id=job_id).observe(duration)
        _record_job_outcome(job_id)
        apply_incidents([], resolve_keys=[_scheduler_failure_key(job_id)])

    elif event_code == EVENT_JOB_ERROR:
        _handle_job_error(event, job_id)


def _handle_job_error(event: object, job_id: str) -> None:
    """Record a failure, capture it in Sentry, and publish an incident."""
    start_time = job_start_times.pop(job_id, None)
    duration = time.time() - start_time if start_time else 0.0

    KBO_SCHEDULER_JOB_TOTAL.labels(job_id=job_id, status="failure").inc()
    KBO_SCHEDULER_JOB_DURATION_SECONDS.labels(job_id=job_id).observe(duration)
    _record_job_outcome(job_id)

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
