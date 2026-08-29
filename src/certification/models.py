"""Data models and protocol definitions for KBO Production Certification Gate."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from src.certification.context import CertificationContext


class GateStatus(StrEnum):
    """Lifecycle evaluation status of a single certification gate."""

    PASS = "PASS"  # noqa: S105
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass(frozen=True)
class GateResult:
    """Immutable execution outcome and machine-readable evidence for a gate."""

    gate_id: str
    name: str
    status: GateStatus
    duration_ms: float
    blocking: bool = True
    metrics: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert gate result to dictionary."""
        return {
            "gate_id": self.gate_id,
            "name": self.name,
            "status": self.status.value,
            "duration_ms": round(self.duration_ms, 2),
            "blocking": self.blocking,
            "metrics": self.metrics,
            "evidence": self.evidence,
            "message": self.message,
        }


class CertificationGate(Protocol):
    """Structural protocol for a pluggable certification gate."""

    gate_id: str
    name: str
    blocking: bool
    dependencies: list[str]

    def run(self, context: CertificationContext) -> GateResult:
        """Execute the gate evaluation and return verifiable results."""
        ...


@dataclass
class CertificationReport:
    """Comprehensive machine-readable production certification report."""

    schema_version: str = "1.0"
    certification_contract: str = "production-v1"
    run_id: str = ""
    target: str = "production"  # production, local, verification
    status: str = "CERTIFIED"  # CERTIFIED, NOT_CERTIFIED, CERTIFIED_WITH_WARNINGS
    started_at: str = ""
    finished_at: str = ""
    git_revision: str = ""
    total_duration_ms: float = 0.0
    blocking_failures: int = 0
    warnings: int = 0
    gates: list[GateResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary for JSON artifact export."""
        return {
            "schema_version": self.schema_version,
            "certification_contract": self.certification_contract,
            "run_id": self.run_id,
            "target": self.target,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "git_revision": self.git_revision,
            "total_duration_ms": round(self.total_duration_ms, 2),
            "blocking_failures": self.blocking_failures,
            "warnings": self.warnings,
            "gates": [g.to_dict() for g in self.gates],
        }


__all__ = [
    "CertificationGate",
    "CertificationReport",
    "GateResult",
    "GateStatus",
]
