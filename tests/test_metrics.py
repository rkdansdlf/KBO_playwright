from __future__ import annotations

from unittest.mock import patch

from src.utils.metrics import (
    KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS,
    KBO_OCI_SYNC_ERRORS_TOTAL,
    KBO_OCI_SYNC_LAG_SECONDS,
    KBO_SCHEDULER_JOB_DURATION_SECONDS,
    KBO_SCHEDULER_JOB_TOTAL,
    start_metrics_server,
)


def test_metrics_definition() -> None:
    """Verify that custom KBO Prometheus metrics are defined correctly."""
    assert KBO_SCHEDULER_JOB_TOTAL._name == "kbo_scheduler_job"
    assert KBO_SCHEDULER_JOB_DURATION_SECONDS._name == "kbo_scheduler_job_duration_seconds"
    assert KBO_OCI_SYNC_LAG_SECONDS._name == "kbo_oci_sync_lag_seconds"
    assert KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS._name == "kbo_oci_last_sync_timestamp_seconds"
    assert KBO_OCI_SYNC_ERRORS_TOTAL._name == "kbo_oci_sync_errors"


@patch("src.utils.metrics.start_http_server")
def test_start_metrics_server(mock_start_http_server: object) -> None:
    """Verify that start_metrics_server calls start_http_server with correct port."""
    start_metrics_server(8888)
    from unittest.mock import Mock

    assert isinstance(mock_start_http_server, Mock)
    mock_start_http_server.assert_called_once_with(8888)
