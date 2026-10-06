"""Canary checks for KBO website selector drift and RAG index consistency."""

from __future__ import annotations

import logging

import requests
from requests import RequestException

from src.notifications.bridge import apply_incidents
from src.scheduler.alerting import alert_success, alert_warning
from src.scheduler.config import SCHEDULER_JOB_EXCEPTIONS
from src.scheduler.locks import _with_db_fail_fast_guard

logger = logging.getLogger("src.scheduler.jobs.sentinel")

HTTP_STATUS_OK = 200

SELECTOR_DRIFT_KEY = "drift:selector:schedule"

#: The audit's "no vector backend answered" code, restated rather than imported.
#: Importing it would pull `src.cli.rag.audit_rag_index` -- and through it
#: `src.db.engine`, which builds the operational engine at import time -- into
#: every scheduler start. A test asserts this stays equal to the audit's own
#: ``EXIT_STORE_UNREACHABLE``, so the two cannot drift apart quietly.
AUDIT_EXIT_STORE_UNREACHABLE = 2


@_with_db_fail_fast_guard
def selector_drift_sentinel_job() -> None:
    """Daily canary check for KBO website selector drift.

    Gated on the operational database because the check's only output is an
    incident. ``apply_incidents`` contains its own persistence errors and returns
    normally, so during an outage the job would fetch the page, reach a verdict,
    and fail to record either outcome -- drift would be detected and then dropped
    on the floor, which is worse than not looking. A full outage is
    ``kbo_db_available``'s story to tell; this job stays quiet so it does not tell
    it wrongly.
    """
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
                    "--config Docs/references/crawler_selector_gate.json --json"
                ),
                metadata={"missing_count": len(missing), "mismatched_count": len(mismatched)},
            ),
        ],
    )


@_with_db_fail_fast_guard
def rag_audit_sentinel_job() -> None:
    """Daily RAG index consistency gate after the sparse catch-up window.

    Gated on the operational database because this job is meaningless without it
    and the audit would otherwise report an unreachable store as an inconsistent
    index. A full outage is ``kbo_db_available``'s story to tell; this job stays
    quiet so it does not tell it wrongly.
    """
    try:
        from src.cli.rag.audit_rag_index import main as audit_main

        exit_code = audit_main(["--require-nonempty", "--require-postings", "--json"])
    except SCHEDULER_JOB_EXCEPTIONS:
        logger.exception("[Sentinel] RAG index audit crashed")
        alert_warning("rag_audit_sentinel", "RAG index audit crashed; see scheduler logs")
        return

    if exit_code == 0:
        logger.info("[Sentinel] RAG index audit passed (sparse postings and vectors consistent).")
        # Resolve the warning an earlier run may have opened. Nothing else owns
        # the `scheduler:rag_audit_sentinel:warning` key -- the lifecycle listener
        # only clears `:failed` -- so without this the incident stays OPEN
        # forever and the runbook's "an incident closes when its check recovers"
        # would be false for this check alone.
        alert_success("rag_audit_sentinel", "RAG index audit passed")
        return

    alert_warning("rag_audit_sentinel", _audit_failure_message(exit_code))


def _audit_failure_message(exit_code: int) -> str:
    """Describe an audit failure without blaming the index for a dead store.

    The audit returns two different non-zero codes, and collapsing them told the
    operator their chunks were missing embeddings -- then suggested a catch-up
    that writes to the store -- in the one case where the store itself could not
    be reached. Same words, wrong half of the system.
    """
    if exit_code == AUDIT_EXIT_STORE_UNREACHABLE:
        return (
            "RAG audit could not reach a vector backend (exit 2); the index was NOT examined. "
            "Check DATABASE_URL / PGVECTOR_URL reachability before suspecting the index -- "
            "build_oracle_sparse_index --catch-up would fail too while the store is down."
        )
    return (
        f"RAG index audit failed (exit {exit_code}); retrievable chunks are missing "
        "embeddings or sparse postings. Run build_oracle_sparse_index --catch-up if needed."
    )
