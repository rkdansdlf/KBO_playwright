"""Alerting helper functions for scheduler tasks."""

from __future__ import annotations

import logging

from src.scheduler.config import ALERT_EXCEPTIONS
from src.utils.alerting import SlackWebhookClient

logger = logging.getLogger("src.scheduler.alerting")


def alert_failure(retry_state: object) -> None:
    """Send Slack alert on job retry failure."""
    fn_name = "unknown"
    fn = getattr(retry_state, "fn", None)
    if fn is not None:
        fn_name = getattr(fn, "__name__", "unknown")
    outcome = getattr(retry_state, "outcome", None)
    exc = outcome.exception() if outcome else "Unknown error"
    attempt = getattr(retry_state, "attempt_number", "?")
    logger.error("Job %s failed on attempt %s: %s", fn_name, attempt, exc)
    try:
        SlackWebhookClient.send_error_alert(
            f"🚨 <b>Scheduler Job Permanently Failed</b>\nFunction: <code>{fn_name}</code>\n"
            f"Attempts: {attempt}\nError: {exc}",
        )
    except ALERT_EXCEPTIONS:
        logger.exception("Failed to send Slack alert for %s", fn_name)


def alert_warning(func_name: str, details: str | None = None) -> None:
    """Send Slack warning alert."""
    logger.warning("Job %s emitted warning: %s", func_name, details)
    try:
        SlackWebhookClient.send_alert(
            f"⚠️ <b>Scheduler Job Warning</b>\nFunction: <code>{func_name}</code>\n"
            f"Details: {details or 'No details provided'}",
        )
    except ALERT_EXCEPTIONS:
        logger.exception("Failed to send Slack warning for %s", func_name)


def alert_success(func_name: str, details: str | None = None) -> None:
    """Send Slack success notification."""
    logger.info("Job %s succeeded: %s", func_name, details)
    try:
        SlackWebhookClient.send_alert(
            f"✅ <b>Scheduler Job Completed</b>\nFunction: <code>{func_name}</code>\nDetails: {details or 'Success'}",
        )
    except ALERT_EXCEPTIONS:
        logger.exception("Failed to send Slack success notification for %s", func_name)
