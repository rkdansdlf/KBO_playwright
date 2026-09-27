"""Canary checks for KBO website selector drift and RAG index consistency."""

from __future__ import annotations

import logging

import requests
from requests import RequestException

from src.notifications.bridge import apply_incidents
from src.scheduler.alerting import alert_warning
from src.scheduler.config import SCHEDULER_JOB_EXCEPTIONS

logger = logging.getLogger("src.scheduler.jobs.sentinel")

HTTP_STATUS_OK = 200

SELECTOR_DRIFT_KEY = "drift:selector:schedule"


def selector_drift_sentinel_job() -> None:
    """Daily canary check for KBO website selector drift."""
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
        _publish_selector_drift(report)
    except (RequestException, RuntimeError, ValueError, TypeError, OSError):
        logger.exception("[Sentinel] Selector drift canary check failed")


def _publish_selector_drift(report: object) -> None:
    """Open or recover the schedule selector-drift incident."""
    from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource

    if getattr(report, "is_healthy", False):
        logger.info("[Sentinel] Schedule page contract healthy (drift check passed).")
        apply_incidents([], resolve_keys=[SELECTOR_DRIFT_KEY])
        return

    missing = list(getattr(report, "missing_selectors", []) or [])
    mismatched = list(getattr(report, "mismatched_columns", []) or [])
    logger.warning(
        "[Sentinel] Selector drift detected on schedule page: missing_selectors=%s mismatched_columns=%s",
        missing,
        mismatched,
    )
    apply_incidents(
        [
            AlertEvent(
                source=AlertSource.DRIFT,
                component="selector:schedule",
                severity=AlertSeverity.ERROR,
                title="KBO 스케줄 페이지 selector drift 감지",
                message=f"missing={missing} columns={mismatched}",
                incident_key=SELECTOR_DRIFT_KEY,
                remediation=(
                    "python3 -m src.cli.crawler_selector_gate "
                    "--config Docs/references/crawler_selector_gate.json --json",
                ),
                metadata={"missing_count": len(missing), "mismatched_count": len(mismatched)},
            ),
        ],
    )


def rag_audit_sentinel_job() -> None:
    """Daily RAG index consistency gate after the sparse catch-up window."""
    try:
        from src.cli.rag.audit_rag_index import main as audit_main

        exit_code = audit_main(["--require-nonempty", "--require-postings", "--json"])
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("[Sentinel] RAG index audit crashed")
        alert_warning("rag_audit_sentinel", "RAG index audit crashed; see scheduler logs")
        return

    if exit_code == 0:
        logger.info("[Sentinel] RAG index audit passed (sparse postings and vectors consistent).")
        return

    alert_warning(
        "rag_audit_sentinel",
        f"RAG index audit failed (exit {exit_code}); retrievable chunks are missing "
        "embeddings or sparse postings. Run build_oracle_sparse_index --catch-up if needed.",
    )
