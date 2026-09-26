"""Prometheus metrics exporter for KBO crawler."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from prometheus_client import Counter, Gauge, Histogram, start_http_server

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

# Scheduler job metrics
KBO_SCHEDULER_JOB_TOTAL = Counter(
    "kbo_scheduler_job_total",
    "Total count of scheduler jobs executed",
    ["job_id", "status"],  # status can be 'success' or 'failure'
)

KBO_SCHEDULER_JOB_DURATION_SECONDS = Histogram(
    "kbo_scheduler_job_duration_seconds",
    "Time spent executing scheduler jobs",
    ["job_id"],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 3600.0, float("inf")),
)

# Lock contention metrics
KBO_SCHEDULER_LOCK_SKIP_TOTAL = Counter(
    "kbo_scheduler_lock_skip_total",
    "Total count of scheduler jobs skipped due to lock contention",
    ["job_id", "lock"],  # lock: sqlite_writer | live_refresh
)

# Auto-Healer metrics
KBO_AUTO_HEALER_RECOVERED_TOTAL = Counter(
    "kbo_auto_healer_recovered_total",
    "Total count of games successfully auto-healed",
    ["type"],  # label values: 'stuck', 'inconsistent', 'pbp'
)

KBO_AUTO_HEALER_UNRESOLVED_TOTAL = Counter(
    "kbo_auto_healer_unresolved_total",
    "Total count of games that failed auto-healing",
    ["type"],  # label values: 'stuck', 'pbp'
)

# API Cache metrics
KBO_API_CACHE_REQUESTS_TOTAL = Counter(
    "kbo_api_cache_requests_total",
    "Total count of API endpoint cache requests",
    ["endpoint", "result"],  # result: 'hit' | 'miss'
)


# Notification / incident metrics
KBO_NOTIFICATION_DISPATCH_TOTAL = Counter(
    "kbo_notification_dispatch_total",
    "Total count of notification delivery attempts",
    ["channel", "status"],  # status: SENT | FAILED | SKIPPED_UNCONFIGURED | DRY_RUN
)

KBO_NOTIFICATION_DISPATCH_FAILURES_TOTAL = Counter(
    "kbo_notification_dispatch_failures_total",
    "Total count of failed notification deliveries",
    ["channel"],
)

KBO_NOTIFICATION_DISPATCH_DURATION_SECONDS = Histogram(
    "kbo_notification_dispatch_duration_seconds",
    "Time spent delivering notifications",
    ["channel"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, float("inf")),
)

KBO_NOTIFICATION_OPEN_INCIDENTS = Gauge(
    "kbo_notification_open_incidents",
    "Currently open notification incidents",
    ["source", "severity"],
)

KBO_NOTIFICATION_DELIVERIES_PERSISTED_TOTAL = Counter(
    "kbo_notification_deliveries_persisted_total",
    "Total count of delivery audit rows persisted",
    ["channel"],
)

KBO_NOTIFICATION_DELIVERY_AUDIT_FAILURES_TOTAL = Counter(
    "kbo_notification_delivery_audit_failures_total",
    "Total count of delivery audit writes that failed (transport result is unaffected)",
)

KBO_NOTIFICATION_DELIVERY_RETRIES_TOTAL = Counter(
    "kbo_notification_delivery_retries_total",
    "Total count of deliveries that required more than one transport attempt",
    ["channel"],
)


# Dead letter queue metrics: gauges are a projection of current DB state and are
# refreshed after each worker/recovery/operator batch; counters accumulate events.
# Labels are intentionally limited to status/crawler/error_code/outcome/action;
# identifiers (dlq_id, run_id, game_id, target_id) are never used as labels.
KBO_DLQ_LETTERS = Gauge(
    "kbo_crawl_dlq_letters",
    "Current crawl dead letters by status and crawler",
    ["status", "crawler"],
)

KBO_DLQ_DUE_LETTERS = Gauge(
    "kbo_crawl_dlq_due_letters",
    "Pending dead letters currently due for retry",
)

KBO_DLQ_STALE_RETRYING_LETTERS = Gauge(
    "kbo_crawl_dlq_stale_retrying_letters",
    "Dead letters stuck retrying past the stale cutoff",
)

KBO_DLQ_OLDEST_PENDING_AGE_SECONDS = Gauge(
    "kbo_crawl_dlq_oldest_pending_age_seconds",
    "Age in seconds of the oldest pending dead letter",
)

KBO_DLQ_FAILURES_TOTAL = Counter(
    "kbo_crawl_dlq_failures_total",
    "Total dead letters enqueued",
    ["crawler", "error_code"],
)

KBO_DLQ_RETRY_ATTEMPTS_TOTAL = Counter(
    "kbo_crawl_dlq_retry_attempts_total",
    "Total dead letter replay attempts started",
)

KBO_DLQ_RETRY_OUTCOMES_TOTAL = Counter(
    "kbo_crawl_dlq_retry_outcomes_total",
    "Total dead letter replay outcomes by crawler",
    ["crawler", "outcome"],
)

KBO_DLQ_RECOVERY_ACTIONS_TOTAL = Counter(
    "kbo_crawl_dlq_recovery_actions_total",
    "Total dead letter recovery actions",
    ["action"],
)


def record_notification_dispatch(channel: str, status: str, duration_seconds: float) -> None:
    """Record one notification delivery attempt and its latency."""
    KBO_NOTIFICATION_DISPATCH_TOTAL.labels(channel=channel, status=status).inc()
    KBO_NOTIFICATION_DISPATCH_DURATION_SECONDS.labels(channel=channel).observe(max(0.0, duration_seconds))
    if status == "FAILED":
        KBO_NOTIFICATION_DISPATCH_FAILURES_TOTAL.labels(channel=channel).inc()


def record_notification_delivery_persisted(channel: str, *, attempt_count: int) -> None:
    """Record a persisted delivery audit row and flag transport retries."""
    KBO_NOTIFICATION_DELIVERIES_PERSISTED_TOTAL.labels(channel=channel).inc()
    if attempt_count > 1:
        KBO_NOTIFICATION_DELIVERY_RETRIES_TOTAL.labels(channel=channel).inc()


def record_notification_delivery_audit_failure() -> None:
    """Record a failed delivery audit write (the transport outcome is preserved)."""
    KBO_NOTIFICATION_DELIVERY_AUDIT_FAILURES_TOTAL.inc()


def record_open_incidents(counts: Mapping[tuple[str, str], int]) -> None:
    """Set the open-incident gauge, clearing labels that are no longer present."""
    KBO_NOTIFICATION_OPEN_INCIDENTS.clear()
    for (source, severity), count in counts.items():
        KBO_NOTIFICATION_OPEN_INCIDENTS.labels(source=source, severity=severity).set(count)


def record_dlq_enqueued(crawler: str, error_code: str) -> None:
    """Record a newly enqueued dead letter."""
    KBO_DLQ_FAILURES_TOTAL.labels(crawler=crawler, error_code=error_code).inc()


def record_dlq_retry_attempt() -> None:
    """Record the start of one dead letter replay attempt."""
    KBO_DLQ_RETRY_ATTEMPTS_TOTAL.inc()


def record_dlq_retry_outcome(crawler: str, outcome: str) -> None:
    """Record a dead letter replay outcome (resolved/pending/exhausted/error/conflict)."""
    KBO_DLQ_RETRY_OUTCOMES_TOTAL.labels(crawler=crawler, outcome=outcome).inc()


def record_dlq_recovery_action(action: str) -> None:
    """Record one dead letter recovery action."""
    KBO_DLQ_RECOVERY_ACTIONS_TOTAL.labels(action=action).inc()


def refresh_dlq_state_metrics(
    *,
    status_crawler_counts: Mapping[tuple[str, str], int],
    due: int,
    stale_retrying: int,
    oldest_pending_age_seconds: float,
) -> None:
    """Set dead letter state gauges from a fresh DB projection."""
    KBO_DLQ_LETTERS.clear()
    for (status, crawler), count in status_crawler_counts.items():
        KBO_DLQ_LETTERS.labels(status=status, crawler=crawler).set(count)
    KBO_DLQ_DUE_LETTERS.set(due)
    KBO_DLQ_STALE_RETRYING_LETTERS.set(stale_retrying)
    KBO_DLQ_OLDEST_PENDING_AGE_SECONDS.set(oldest_pending_age_seconds)


_db_availability_registered = False


def register_database_availability_collector(engine: object | None = None) -> None:
    """Register the on-scrape DB availability/latency collector exactly once."""
    global _db_availability_registered  # noqa: PLW0603
    if _db_availability_registered:
        return
    from prometheus_client.core import REGISTRY

    from src.monitoring.db_availability import DatabaseAvailabilityCollector

    REGISTRY.register(DatabaseAvailabilityCollector(engine))
    _db_availability_registered = True


def record_api_cache(endpoint: str, *, hit: bool) -> None:
    """Record API cache hit or miss metric."""
    label_value = "hit" if hit else "miss"
    KBO_API_CACHE_REQUESTS_TOTAL.labels(endpoint=endpoint, result=label_value).inc()


def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics exporter HTTP server."""
    register_database_availability_collector()
    try:
        start_http_server(port)
        logger.info("Prometheus metrics exporter server started on port %d", port)
    except OSError:
        logger.exception("Failed to start Prometheus HTTP server on port %d", port)
