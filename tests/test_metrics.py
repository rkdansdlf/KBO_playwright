from __future__ import annotations

from unittest.mock import patch

from src.utils.metrics import (
    KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS,
    KBO_OCI_SYNC_ERRORS_TOTAL,
    KBO_OCI_SYNC_LAG_SECONDS,
    KBO_OCI_SYNCED_RECORDS_TOTAL,
    KBO_OCI_TABLE_SYNC_LAG_SECONDS,
    KBO_SCHEDULER_JOB_DURATION_SECONDS,
    KBO_SCHEDULER_JOB_TOTAL,
    record_oci_sync_metric,
    start_metrics_server,
)


def test_metrics_definition() -> None:
    """Verify that custom KBO Prometheus metrics are defined correctly."""
    assert KBO_SCHEDULER_JOB_TOTAL._name == "kbo_scheduler_job"
    assert KBO_SCHEDULER_JOB_DURATION_SECONDS._name == "kbo_scheduler_job_duration_seconds"
    assert KBO_OCI_SYNC_LAG_SECONDS._name == "kbo_oci_sync_lag_seconds"
    assert KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS._name == "kbo_oci_last_sync_timestamp_seconds"
    assert KBO_OCI_SYNC_ERRORS_TOTAL._name == "kbo_oci_sync_errors"
    assert KBO_OCI_SYNCED_RECORDS_TOTAL._name == "kbo_oci_synced_records"
    assert KBO_OCI_TABLE_SYNC_LAG_SECONDS._name == "kbo_oci_table_sync_lag_seconds"


def test_record_oci_sync_metric() -> None:
    """Verify record_oci_sync_metric increments total synced counter and updates timestamp."""
    val_before = KBO_OCI_SYNCED_RECORDS_TOTAL.labels(table="game")._value.get()
    record_oci_sync_metric("game", 15)
    val_after = KBO_OCI_SYNCED_RECORDS_TOTAL.labels(table="game")._value.get()
    assert val_after == val_before + 15
    assert KBO_OCI_LAST_SYNC_TIMESTAMP_SECONDS._value.get() > 0

    # zero count should be no-op
    record_oci_sync_metric("game", 0)
    assert KBO_OCI_SYNCED_RECORDS_TOTAL.labels(table="game")._value.get() == val_after


@patch("src.utils.metrics.start_http_server")
def test_start_metrics_server(mock_start_http_server: object) -> None:
    """Verify that start_metrics_server calls start_http_server with correct port."""
    start_metrics_server(8888)
    from unittest.mock import Mock

    assert isinstance(mock_start_http_server, Mock)
    mock_start_http_server.assert_called_once_with(8888)
