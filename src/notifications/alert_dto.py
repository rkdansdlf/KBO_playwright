"""Domain contract for the in-process alert manager.

This module defines the vocabulary that every monitoring producer maps onto:

* :class:`AlertSource` — which subsystem produced the signal.
* :class:`AlertSeverity` — the canonical severity axis (absorbing the seven
  legacy vocabularies: ``GateStatus``, ``DiagnosticSeverity``,
  ``AnomalySeverity``, ``DriftSeverity``, gap severity, ``NotificationPriority``
  and plain booleans).
* :class:`AlertStatus` — the incident lifecycle.
* :class:`AlertEvent` — the normalized event published by a producer.
* :class:`IncidentTransition` — the decision the :class:`IncidentManager` made.

Producers publish an :class:`AlertEvent`; they never render a message or choose
a transport. The incident manager owns dedup, cooldown and lifecycle, and the
dispatcher owns transport.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


def utcnow() -> datetime:
    """Return the current time as a naive UTC datetime.

    Matches the database datetime convention used by
    :mod:`src.services.crawl_run_service`.
    """
    return datetime.now(UTC).replace(tzinfo=None)


class AlertSource(StrEnum):
    """Subsystem that produced an alert event."""

    DATABASE = "database"
    CRAWLER = "crawler"
    INTEGRITY = "integrity"
    QUALITY = "quality"
    FRESHNESS = "freshness"
    DRIFT = "drift"
    RAG = "rag"
    RECOVERY = "recovery"
    SCHEDULER = "scheduler"
    PIPELINE = "pipeline"
    REPORT = "report"
    SYSTEM = "system"


class AlertSeverity(StrEnum):
    """Canonical severity; drives routing, cooldown and escalation."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


_SEVERITY_ORDER: dict[AlertSeverity, int] = {
    AlertSeverity.INFO: 0,
    AlertSeverity.WARNING: 1,
    AlertSeverity.ERROR: 2,
    AlertSeverity.CRITICAL: 3,
}


def severity_rank(severity: AlertSeverity) -> int:
    """Return a monotonic rank so severities can be compared and escalated."""
    return _SEVERITY_ORDER[severity]


def severity_at_least(candidate: AlertSeverity, floor: AlertSeverity) -> bool:
    """Return whether ``candidate`` is at least as severe as ``floor``."""
    return severity_rank(candidate) >= severity_rank(floor)


class AlertStatus(StrEnum):
    """Incident lifecycle state.

    ``NOT_APPLICABLE`` is a non-alertable gate: a producer that legitimately
    skipped a check (missing table before its migration, source unavailable)
    reports this instead of a failure so it never opens or resolves an incident.
    """

    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RECOVERED = "RECOVERED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class AlertDecision(StrEnum):
    """What the incident manager decided to do with an event."""

    NEW = "NEW"
    REPEAT = "REPEAT"
    SUPPRESSED = "SUPPRESSED"
    ESCALATED = "ESCALATED"
    RECOVERED = "RECOVERED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    NOOP = "NOOP"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class AlertEvent:
    """Normalized alert event published by a monitoring producer.

    ``incident_key`` is a *semantic* identity (for example
    ``integrity:game_stats:20260925``) rather than a hash of the rendered
    message, so reworded messages still map to the same incident.
    """

    source: AlertSource
    component: str
    severity: AlertSeverity
    title: str
    message: str
    incident_key: str
    occurred_at: datetime = field(default_factory=utcnow)
    remediation: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    alert_type: str = ""

    def details_hash(self) -> str:
        """Hash the mutable detail payload to detect materially changed content."""
        payload = f"{self.message}|{self.alert_type}|{sorted(self.metadata.items())}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        """Convert the event to a serializable dictionary."""
        return {
            "source": self.source.value,
            "component": self.component,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "incident_key": self.incident_key,
            "occurred_at": self.occurred_at.isoformat(),
            "remediation": list(self.remediation),
            "metadata": self.metadata,
            "alert_type": self.alert_type,
        }


@dataclass(frozen=True)
class IncidentTransition:
    """Outcome of applying an event (or resolution) to the incident ledger."""

    incident_key: str
    source: AlertSource
    severity: AlertSeverity
    state: AlertStatus
    decision: AlertDecision
    occurrence_count: int
    first_opened_at: datetime
    last_seen_at: datetime
    previous_state: AlertStatus | None = None
    previous_severity: AlertSeverity | None = None
    duration_seconds: float = 0.0
    incident_id: int | None = None

    @property
    def is_active(self) -> bool:
        """Return whether the incident is still open or acknowledged."""
        return self.state in (AlertStatus.OPEN, AlertStatus.ACKNOWLEDGED)

    @property
    def should_notify(self) -> bool:
        """Return whether the decision warrants delivering a message."""
        return self.decision in (
            AlertDecision.NEW,
            AlertDecision.REPEAT,
            AlertDecision.ESCALATED,
            AlertDecision.RECOVERED,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert the transition to a serializable dictionary."""
        return {
            "incident_key": self.incident_key,
            "source": self.source.value,
            "severity": self.severity.value,
            "state": self.state.value,
            "decision": self.decision.value,
            "occurrence_count": self.occurrence_count,
            "first_opened_at": self.first_opened_at.isoformat(),
            "last_seen_at": self.last_seen_at.isoformat(),
            "previous_state": self.previous_state.value if self.previous_state else None,
            "previous_severity": self.previous_severity.value if self.previous_severity else None,
            "duration_seconds": round(self.duration_seconds, 3),
            "incident_id": self.incident_id,
        }
