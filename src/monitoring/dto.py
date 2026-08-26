"""Standard Data Transfer Objects (DTOs) for Anomaly Detection and Monitoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class AnomalyType(StrEnum):
    """Categories of detected system and data anomalies."""

    SUDDEN_DROP = "sudden_drop"
    SPIKE = "spike"
    STALE_DATA = "stale_data"
    SCHEMA_DRIFT = "schema_drift"
    SELECTOR_ERROR = "selector_error"
    LOCK_STARVATION = "lock_starvation"


class AnomalySeverity(StrEnum):
    """Severity levels for anomaly events."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class AnomalyDetectionRule:
    """Specification of an anomaly detection rule."""

    rule_name: str
    metric_name: str
    anomaly_type: AnomalyType
    threshold: float
    window_size: int = 7
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert rule specification to dictionary."""
        return {
            "rule_name": self.rule_name,
            "metric_name": self.metric_name,
            "anomaly_type": self.anomaly_type.value,
            "threshold": self.threshold,
            "window_size": self.window_size,
            "enabled": self.enabled,
        }


@dataclass
class AnomalyEvent:
    """An individual anomaly event occurrence."""

    event_id: str
    rule_name: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    observed_value: float
    expected_range: tuple[float, float]
    detected_at: str
    details: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert anomaly event to dictionary."""
        return {
            "event_id": self.event_id,
            "rule_name": self.rule_name,
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity.value,
            "observed_value": round(self.observed_value, 4),
            "expected_range": [round(self.expected_range[0], 4), round(self.expected_range[1], 4)],
            "detected_at": self.detected_at,
            "details": self.details,
        }


@dataclass
class AnomalyAuditReport:
    """Aggregated report of all evaluated metrics and detected anomalies."""

    total_metrics_evaluated: int
    anomalies_detected: int
    overall_status: str  # HEALTHY, WARNING, CRITICAL
    events: list[AnomalyEvent] = field(default_factory=list)
    evaluated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert audit report to dictionary."""
        return {
            "total_metrics_evaluated": self.total_metrics_evaluated,
            "anomalies_detected": self.anomalies_detected,
            "overall_status": self.overall_status,
            "events": [e.to_dict() for e in self.events],
            "evaluated_at": self.evaluated_at,
        }
