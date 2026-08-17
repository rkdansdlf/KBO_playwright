"""Canary check for KBO website selector drift."""

from __future__ import annotations

import logging

import requests
from requests import RequestException

from src.scheduler.config import ALERT_EXCEPTIONS

logger = logging.getLogger("src.scheduler.jobs.sentinel")

HTTP_STATUS_OK = 200


def selector_drift_sentinel_job() -> None:
    """Daily Canary Check for KBO Website Selector Drift."""
    try:
        from src.monitoring.selector_drift_sentinel import (
            PageContract,
            create_default_kbo_sentinel,
        )

        sentinel = create_default_kbo_sentinel()
        sentinel.register_contract(
            PageContract(
                page_name="schedule",
                required_selectors=(".tbl",),
                min_table_columns={".tbl": 2},
            ),
        )

        page_url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        response = requests.get(page_url, timeout=20)
        if response.status_code != HTTP_STATUS_OK:
            logger.warning("[Sentinel] KBO schedule page fetch returned HTTP %s", response.status_code)
            return

        report = sentinel.check_html("schedule", response.text)
        if report.is_healthy:
            logger.info("[Sentinel] Schedule page contract healthy (drift check passed).")
            return

        logger.warning(
            "[Sentinel] Selector drift detected on schedule page: missing_selectors=%s mismatched_columns=%s",
            list(report.missing_selectors),
            list(report.mismatched_columns),
        )
        try:
            from src.utils.alerting import SlackWebhookClient

            SlackWebhookClient.send_alert(
                f"⚠️ Selector drift detected on KBO schedule page: "
                f"missing={list(report.missing_selectors)} columns={list(report.mismatched_columns)}",
            )
        except ALERT_EXCEPTIONS:
            logger.exception("[Sentinel] Failed to send drift alert")
    except (RequestException, RuntimeError, ValueError, TypeError, OSError):
        logger.exception("[Sentinel] Selector drift canary check failed")
