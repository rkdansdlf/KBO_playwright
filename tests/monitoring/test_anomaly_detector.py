"""Unit tests for src.monitoring.anomaly_detector."""

from __future__ import annotations

from src.monitoring.anomaly_detector import AnomalyDetector
from src.monitoring.dto import AnomalySeverity, AnomalyType


def test_detect_time_series_drop() -> None:
    detector = AnomalyDetector(default_z_threshold=2.0)
    series = [100.0, 102.0, 99.0, 101.0, 98.0, 100.0, 0.0]  # Sudden drop to 0
    events = detector.detect_time_series_anomalies("test_metric", series)

    assert len(events) == 1
    assert events[0].anomaly_type == AnomalyType.SUDDEN_DROP
    assert events[0].severity == AnomalySeverity.CRITICAL


def test_detect_time_series_spike() -> None:
    detector = AnomalyDetector(default_z_threshold=2.0)
    series = [10.0, 11.0, 9.0, 10.0, 12.0, 10.0, 150.0]  # Massive spike
    events = detector.detect_time_series_anomalies("test_metric", series)

    assert len(events) == 1
    assert events[0].anomaly_type == AnomalyType.SPIKE
    assert events[0].observed_value == 150.0


def test_detect_freshness_anomalies() -> None:
    detector = AnomalyDetector()
    stale_map = {
        "game": 2.0,
        "team_standings_daily": 30.0,  # Stale > 24h
    }
    events = detector.detect_freshness_anomalies(stale_map, max_hours=24.0)

    assert len(events) == 1
    assert events[0].anomaly_type == AnomalyType.STALE_DATA
    assert events[0].rule_name == "freshness_team_standings_daily"


def test_detect_lock_starvation() -> None:
    detector = AnomalyDetector()
    events = detector.detect_lock_starvation(skip_count=8, threshold=5)

    assert len(events) == 1
    assert events[0].anomaly_type == AnomalyType.LOCK_STARVATION
    assert events[0].observed_value == 8.0


def test_audit_snapshot() -> None:
    detector = AnomalyDetector()
    snapshot = {
        "series": {
            "daily_games": [5.0, 5.0, 5.0, 5.0, 5.0],
        },
        "stale_hours": {
            "game": 1.0,
        },
        "selector_error_rate": 0.0,
        "lock_skips": 0,
    }
    report = detector.audit_snapshot(snapshot)
    assert report.overall_status == "HEALTHY"
    assert report.anomalies_detected == 0
