"""Unit tests for src.monitoring.dto."""

from __future__ import annotations

from src.monitoring.dto import (
    AnomalyAuditReport,
    AnomalyDetectionRule,
    AnomalyEvent,
    AnomalySeverity,
    AnomalyType,
)


def test_anomaly_type_values() -> None:
    assert AnomalyType.SUDDEN_DROP == "sudden_drop"
    assert AnomalyType.SPIKE == "spike"
    assert AnomalyType.STALE_DATA == "stale_data"
    assert AnomalyType.SCHEMA_DRIFT == "schema_drift"
    assert AnomalyType.SELECTOR_ERROR == "selector_error"
    assert AnomalyType.LOCK_STARVATION == "lock_starvation"


def test_anomaly_severity_values() -> None:
    assert AnomalySeverity.LOW == "LOW"
    assert AnomalySeverity.MEDIUM == "MEDIUM"
    assert AnomalySeverity.HIGH == "HIGH"
    assert AnomalySeverity.CRITICAL == "CRITICAL"


def test_anomaly_detection_rule_to_dict() -> None:
    rule = AnomalyDetectionRule(
        rule_name="batting_drop_rule",
        metric_name="batting_count",
        anomaly_type=AnomalyType.SUDDEN_DROP,
        threshold=3.0,
    )
    d = rule.to_dict()
    assert d["rule_name"] == "batting_drop_rule"
    assert d["anomaly_type"] == "sudden_drop"
    assert d["threshold"] == 3.0


def test_anomaly_event_to_dict() -> None:
    event = AnomalyEvent(
        event_id="evt_001",
        rule_name="batting_drop_rule",
        anomaly_type=AnomalyType.SUDDEN_DROP,
        severity=AnomalySeverity.CRITICAL,
        observed_value=0.0,
        expected_range=(100.0, 200.0),
        detected_at="2026-08-26T23:00:00Z",
        details="No records inserted",
    )
    d = event.to_dict()
    assert d["event_id"] == "evt_001"
    assert d["severity"] == "CRITICAL"
    assert d["observed_value"] == 0.0


def test_anomaly_audit_report_to_dict() -> None:
    report = AnomalyAuditReport(
        total_metrics_evaluated=5,
        anomalies_detected=1,
        overall_status="WARNING",
        events=[],
        evaluated_at="2026-08-26T23:00:00Z",
    )
    d = report.to_dict()
    assert d["total_metrics_evaluated"] == 5
    assert d["anomalies_detected"] == 1
    assert d["overall_status"] == "WARNING"
