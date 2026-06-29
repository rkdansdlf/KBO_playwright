"""Prometheus metrics exporter for KBO crawler."""

from __future__ import annotations

import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

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

# OCI synchronization metrics
KBO_OCI_SYNC_LAG_SECONDS = Gauge(
    "kbo_oci_sync_lag_seconds",
    "Difference in seconds between the latest updated_at in SQLite and OCI",
)

KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS = Gauge(
    "kbo_oci_last_sync_timestamp_seconds",
    "Timestamp of the last successful OCI sync operation",
)

KBO_OCI_SYNC_ERRORS_TOTAL = Counter(
    "kbo_oci_sync_errors_total",
    "Total count of OCI sync errors",
)


def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics exporter HTTP server."""
    try:
        start_http_server(port)
        logger.info("Prometheus metrics exporter server started on port %d", port)
    except OSError:
        logger.exception("Failed to start Prometheus HTTP server on port %d", port)
