"""Job lifecycle listener and metrics reporting for the KBO scheduler."""

from __future__ import annotations

import logging
import os
import time

import sentry_sdk
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_SUBMITTED

from src.scheduler.config import ALERT_EXCEPTIONS
from src.utils.alerting import SlackWebhookClient
from src.utils.metrics import (
    KBO_SCHEDULER_JOB_DURATION_SECONDS,
    KBO_SCHEDULER_JOB_TOTAL,
)

logger = logging.getLogger("src.scheduler.metrics")

job_start_times: dict[str, float] = {}
_LAST_ALERT_SENT_AT: dict[str, float] = {}
ALERT_THROTTLE_SECONDS = float(os.getenv("SCHEDULER_ALERT_THROTTLE_SECONDS", "300"))


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
        _LAST_ALERT_SENT_AT.pop(job_id, None)

    elif event_code == EVENT_JOB_ERROR:
        start_time = job_start_times.pop(job_id, None)
        duration = time.time() - start_time if start_time else 0.0

        KBO_SCHEDULER_JOB_TOTAL.labels(job_id=job_id, status="failure").inc()
        KBO_SCHEDULER_JOB_DURATION_SECONDS.labels(job_id=job_id).observe(duration)

        exc = getattr(event, "exception", None)
        if exc:
            import traceback

            tb = "".join(traceback.format_exception(type(exc), exc, getattr(exc, "__traceback__", None)))
            logger.error("Job %s failed: %s", job_id, exc)

            sentry_sdk.capture_exception(exc)

            now = time.time()
            last_alert_time = _LAST_ALERT_SENT_AT.get(job_id, 0.0)
            if now - last_alert_time < ALERT_THROTTLE_SECONDS:
                logger.warning(
                    "Throttling Slack alert for failed job %s (last alert sent %.1fs ago, cooldown is %ds)",
                    job_id,
                    now - last_alert_time,
                    int(ALERT_THROTTLE_SECONDS),
                )
                return

            _LAST_ALERT_SENT_AT[job_id] = now
            try:
                SlackWebhookClient.send_error_alert(f"🚨 <b>Scheduler Job Failed: {job_id}</b>\nError: {exc}\n\n{tb}")
            except ALERT_EXCEPTIONS:
                logger.exception("Failed to send Slack alert for failed job %s", job_id)
