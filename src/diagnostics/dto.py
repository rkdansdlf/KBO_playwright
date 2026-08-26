"""Standard Data Transfer Objects (DTOs) for Unified System Diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class SubsystemType(StrEnum):
    """Subsystem categories audited by the diagnostics engine."""

    DATABASE = "database"
    SCHEDULER = "scheduler"
    CRAWLER = "crawler"
    PIPELINE = "pipeline"
    API_GATEWAY = "api_gateway"
    RAG_VECTOR = "rag_vector"


class DiagnosticSeverity(StrEnum):
    """Severity levels for diagnostic check outcomes."""

    HEALTHY = "HEALTHY"
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class SubsystemCheckItem:
    """Individual diagnostic check outcome."""

    name: str
    subsystem: SubsystemType
    severity: DiagnosticSeverity
    status: str  # OK, WARN, FAIL
    message: str
    metrics: dict[str, Any] = field(default_factory=dict)
    remediation_hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert check item to dictionary."""
        return {
            "name": self.name,
            "subsystem": self.subsystem.value,
            "severity": self.severity.value,
            "status": self.status,
            "message": self.message,
            "metrics": self.metrics,
            "remediation_hint": self.remediation_hint,
        }


@dataclass
class UnifiedDiagnosticsReport:
    """Aggregated multi-subsystem platform diagnostics report."""

    overall_status: str
    total_checks: int
    healthy_count: int
    warning_count: int
    critical_count: int
    checks: list[SubsystemCheckItem] = field(default_factory=list)
    generated_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "overall_status": self.overall_status,
            "total_checks": self.total_checks,
            "healthy_count": self.healthy_count,
            "warning_count": self.warning_count,
            "critical_count": self.critical_count,
            "checks": [c.to_dict() for c in self.checks],
            "generated_at": self.generated_at,
        }
