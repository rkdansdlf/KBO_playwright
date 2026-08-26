"""Intelligent Anomaly Detection Engine for metric streams, freshness, and system stability."""

from __future__ import annotations

import math
import uuid
from datetime import UTC, datetime
from typing import Any

from src.monitoring.dto import (
    AnomalyAuditReport,
    AnomalyEvent,
    AnomalySeverity,
    AnomalyType,
)

MIN_SERIES_LENGTH = 3


class AnomalyDetector:
    """Detects statistical outliers, freshness violations, and system anomalies."""

    def __init__(self, default_z_threshold: float = 3.0) -> None:
        """Initialize anomaly detector."""
        self.default_z_threshold = default_z_threshold

    def detect_time_series_anomalies(
        self,
        metric_name: str,
        values: list[float],
        *,
        z_threshold: float | None = None,
    ) -> list[AnomalyEvent]:
        """Detect statistical outliers in a sequential metric series."""
        if len(values) < MIN_SERIES_LENGTH:
            return []

        threshold = z_threshold or self.default_z_threshold
        baseline = values[:-1]
        latest = values[-1]

        mean = sum(baseline) / len(baseline)
        variance = sum((x - mean) ** 2 for x in baseline) / len(baseline)
        std_dev = math.sqrt(variance)

        if std_dev == 0:
            return []

        z_score = (latest - mean) / std_dev
        lower_bound = max(0.0, mean - threshold * std_dev)
        upper_bound = mean + threshold * std_dev

        events: list[AnomalyEvent] = []
        now_str = datetime.now(UTC).isoformat()

        if z_score > threshold:
            events.append(
                AnomalyEvent(
                    event_id=str(uuid.uuid4())[:8],
                    rule_name=f"{metric_name}_spike_rule",
                    anomaly_type=AnomalyType.SPIKE,
                    severity=AnomalySeverity.HIGH if z_score > (threshold * 1.5) else AnomalySeverity.MEDIUM,
                    observed_value=latest,
                    expected_range=(lower_bound, upper_bound),
                    detected_at=now_str,
                    details=f"Metric spiked: observed {latest:.2f} vs upper bound {upper_bound:.2f} (Z={z_score:.2f})",
                )
            )
        elif z_score < -threshold:
            events.append(
                AnomalyEvent(
                    event_id=str(uuid.uuid4())[:8],
                    rule_name=f"{metric_name}_drop_rule",
                    anomaly_type=AnomalyType.SUDDEN_DROP,
                    severity=AnomalySeverity.CRITICAL if latest == 0 else AnomalySeverity.HIGH,
                    observed_value=latest,
                    expected_range=(lower_bound, upper_bound),
                    detected_at=now_str,
                    details=f"Metric dropped: observed {latest:.2f} vs lower bound {lower_bound:.2f} (Z={z_score:.2f})",
                )
            )

        return events

    def detect_freshness_anomalies(
        self,
        table_stale_hours: dict[str, float],
        *,
        max_hours: float = 24.0,
    ) -> list[AnomalyEvent]:
        """Detect tables that have not been refreshed within acceptable limits."""
        events: list[AnomalyEvent] = []
        now_str = datetime.now(UTC).isoformat()

        for tbl, hours in table_stale_hours.items():
            if hours > max_hours:
                severity = AnomalySeverity.CRITICAL if hours > (max_hours * 2) else AnomalySeverity.HIGH
                events.append(
                    AnomalyEvent(
                        event_id=str(uuid.uuid4())[:8],
                        rule_name=f"freshness_{tbl}",
                        anomaly_type=AnomalyType.STALE_DATA,
                        severity=severity,
                        observed_value=hours,
                        expected_range=(0.0, max_hours),
                        detected_at=now_str,
                        details=f"Table '{tbl}' is stale ({hours:.1f}h since update, max allowed: {max_hours:.1f}h)",
                    )
                )
        return events

    def detect_selector_drift(
        self,
        selector_error_rate: float,
        *,
        threshold: float = 0.1,
    ) -> list[AnomalyEvent]:
        """Detect selector drift and crawler DOM structure changes."""
        events: list[AnomalyEvent] = []
        if selector_error_rate > threshold:
            events.append(
                AnomalyEvent(
                    event_id=str(uuid.uuid4())[:8],
                    rule_name="selector_drift_rule",
                    anomaly_type=AnomalyType.SELECTOR_ERROR,
                    severity=AnomalySeverity.HIGH,
                    observed_value=selector_error_rate,
                    expected_range=(0.0, threshold),
                    detected_at=datetime.now(UTC).isoformat(),
                    details=f"Selector error rate {selector_error_rate:.2%} exceeded threshold {threshold:.2%}",
                )
            )
        return events

    def detect_lock_starvation(
        self,
        skip_count: int,
        *,
        threshold: int = 5,
    ) -> list[AnomalyEvent]:
        """Detect scheduler lock contention and worker starvation."""
        events: list[AnomalyEvent] = []
        if skip_count > threshold:
            events.append(
                AnomalyEvent(
                    event_id=str(uuid.uuid4())[:8],
                    rule_name="lock_starvation_rule",
                    anomaly_type=AnomalyType.LOCK_STARVATION,
                    severity=AnomalySeverity.MEDIUM,
                    observed_value=float(skip_count),
                    expected_range=(0.0, float(threshold)),
                    detected_at=datetime.now(UTC).isoformat(),
                    details=f"Scheduler lock skipped {skip_count} times, exceeding threshold {threshold}",
                )
            )
        return events

    def audit_snapshot(self, metrics: dict[str, Any]) -> AnomalyAuditReport:
        """Run comprehensive anomaly detection across a system metrics snapshot."""
        events: list[AnomalyEvent] = []
        total_eval = 0

        # 1. Volume series
        if "series" in metrics:
            for m_name, vals in metrics["series"].items():
                total_eval += 1
                events.extend(self.detect_time_series_anomalies(m_name, vals))

        # 2. Freshness
        if "stale_hours" in metrics:
            total_eval += len(metrics["stale_hours"])
            events.extend(self.detect_freshness_anomalies(metrics["stale_hours"]))

        # 3. Selector errors
        if "selector_error_rate" in metrics:
            total_eval += 1
            events.extend(self.detect_selector_drift(metrics["selector_error_rate"]))

        # 4. Lock skips
        if "lock_skips" in metrics:
            total_eval += 1
            events.extend(self.detect_lock_starvation(metrics["lock_skips"]))

        has_critical = any(e.severity == AnomalySeverity.CRITICAL for e in events)
        has_warning = any(e.severity in (AnomalySeverity.HIGH, AnomalySeverity.MEDIUM) for e in events)

        if has_critical:
            overall = "CRITICAL"
        elif has_warning:
            overall = "WARNING"
        else:
            overall = "HEALTHY"

        return AnomalyAuditReport(
            total_metrics_evaluated=total_eval,
            anomalies_detected=len(events),
            overall_status=overall,
            events=events,
            evaluated_at=datetime.now(UTC).isoformat(),
        )
