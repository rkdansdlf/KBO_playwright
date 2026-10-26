"""Standard Data Transfer Objects (DTOs) for the KBO Pipeline and Auto-Healer."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class PipelineDefectType(StrEnum):
    """Enumeration of data defect categories in the KBO pipeline."""

    STUCK_SCHEDULED = "STUCK_SCHEDULED"
    SCORE_MISMATCH = "SCORE_MISMATCH"
    MISSING_STATS = "MISSING_STATS"
    NULL_PLAYER_ID = "NULL_PLAYER_ID"
    UNVERIFIED_PBP = "UNVERIFIED_PBP"
    STALE_DATA = "STALE_DATA"
    UNKNOWN = "UNKNOWN"


class HealingStatus(StrEnum):
    """Outcome vocabulary for a single healing action.

    ``SUCCESS`` claims that *this* process repaired the defect. ``DEFERRED``
    means a remediation exists but belongs to another job, so nothing here was
    repaired -- reporting that as ``SUCCESS`` invents work nobody did.
    ``SKIPPED`` means no handler applies at all.
    """

    SUCCESS = "SUCCESS"
    DEFERRED = "DEFERRED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


HEALING_STATUSES = frozenset(status.value for status in HealingStatus)


@dataclass
class DefectItem:
    """Represents an individual data defect detected in the pipeline."""

    game_id: str
    defect_type: PipelineDefectType
    severity: str = "ERROR"  # "ERROR", "WARNING", "INFO"
    description: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert defect item to a serializable dictionary."""
        return {
            "game_id": self.game_id,
            "defect_type": self.defect_type.value,
            "severity": self.severity,
            "description": self.description,
            "details": self.details,
        }


@dataclass
class DefectReport:
    """Consolidated report of defects detected for a target date or season."""

    target_date: str
    defects: list[DefectItem] = field(default_factory=list)
    timestamp: str = ""

    @property
    def total_defects(self) -> int:
        """Return total number of defects."""
        return len(self.defects)

    @property
    def has_defects(self) -> bool:
        """Return True if any defects were detected."""
        return len(self.defects) > 0

    @property
    def summary_by_type(self) -> dict[str, int]:
        """Return counts of defects grouped by defect type."""
        summary: dict[str, int] = {}
        for d in self.defects:
            key = d.defect_type.value
            summary[key] = summary.get(key, 0) + 1
        return summary

    def to_dict(self) -> dict[str, Any]:
        """Convert defect report to dictionary."""
        return {
            "target_date": self.target_date,
            "total_defects": self.total_defects,
            "summary_by_type": self.summary_by_type,
            "timestamp": self.timestamp,
            "defects": [d.to_dict() for d in self.defects],
        }


@dataclass
class HealingActionSummary:
    """Result of a self-healing action performed on a specific game or entity."""

    game_id: str
    action_taken: str
    status: str = HealingStatus.SUCCESS.value
    error_message: str | None = None
    elapsed_seconds: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def healed(self) -> bool:
        """Return True only when this action actually repaired the defect.

        An unrecognised status is not evidence of a repair, so it counts as
        not healed: the failure mode of over-claiming is what this property
        exists to prevent.
        """
        return self.status == HealingStatus.SUCCESS

    def to_dict(self) -> dict[str, Any]:
        """Convert healing summary to dictionary."""
        return {
            "game_id": self.game_id,
            "action_taken": self.action_taken,
            "status": self.status,
            "healed": self.healed,
            "error_message": self.error_message,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "details": self.details,
        }


@dataclass
class PipelineStageResult:
    """Execution status and metrics for a single stage in the daily pipeline."""

    stage_name: str
    status: str = "SUCCESS"  # "SUCCESS", "WARNING", "FAILED", "SKIPPED"
    duration_seconds: float = 0.0
    metrics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert stage result to dictionary."""
        return {
            "stage_name": self.stage_name,
            "status": self.status,
            "duration_seconds": round(self.duration_seconds, 2),
            "metrics": self.metrics,
            "errors": self.errors,
        }


@dataclass
class PipelineRunSummary:
    """Consolidated summary of a daily data pipeline execution run."""

    run_id: str
    target_date: str
    overall_status: str = "SUCCESS"  # "SUCCESS", "WARNING", "FAILED"
    stages: list[PipelineStageResult] = field(default_factory=list)
    healed_defects: list[HealingActionSummary] = field(default_factory=list)
    total_duration_seconds: float = 0.0
    timestamp: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert pipeline run summary to dictionary."""
        return {
            "run_id": self.run_id,
            "target_date": self.target_date,
            "overall_status": self.overall_status,
            "total_duration_seconds": round(self.total_duration_seconds, 2),
            "timestamp": self.timestamp,
            "stages": [s.to_dict() for s in self.stages],
            "healed_defects": [h.to_dict() for h in self.healed_defects],
        }
