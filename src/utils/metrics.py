"""Prometheus metrics exporter for KBO crawler."""

from __future__ import annotations

import logging

from prometheus_client import Counter, Histogram, start_http_server

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

# DB Lock Duration metrics
KBO_DB_LOCK_WAIT_DURATION_SECONDS = Histogram(
    "kbo_db_lock_wait_duration_seconds",
    "Seconds spent waiting to acquire SQLite or tier locks",
    ["lock_type"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, float("inf")),
)


def record_api_cache(endpoint: str, *, hit: bool) -> None:
    """Record API cache hit or miss metric."""
    label_value = "hit" if hit else "miss"
    KBO_API_CACHE_REQUESTS_TOTAL.labels(endpoint=endpoint, result=label_value).inc()


def record_db_lock_wait(lock_type: str, duration: float) -> None:
    """Record DB or process lock wait duration."""
    KBO_DB_LOCK_WAIT_DURATION_SECONDS.labels(lock_type=lock_type).observe(duration)


def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics exporter HTTP server."""
    try:
        start_http_server(port)
        logger.info("Prometheus metrics exporter server started on port %d", port)
    except OSError:
        logger.exception("Failed to start Prometheus HTTP server on port %d", port)
