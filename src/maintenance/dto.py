"""Standard Data Transfer Objects (DTOs) for Maintenance Tasks and Orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class MaintenanceTaskType(StrEnum):
    """Types of maintenance and database repair tasks."""

    PA_AUDIT = "pa_audit"
    NULL_PLAYER_IDS = "null_player_ids"
    DATA_CLEANUP = "data_cleanup"
    WAL_CHECKPOINT = "wal_checkpoint"
    CUSTOM = "custom"


@dataclass
class MaintenanceTaskMeta:
    """Metadata describing a single maintenance task."""

    task_name: str
    task_type: MaintenanceTaskType
    description: str
    safe_mode_supported: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert task metadata to dictionary."""
        return {
            "task_name": self.task_name,
            "task_type": self.task_type.value,
            "description": self.description,
            "safe_mode_supported": self.safe_mode_supported,
        }


@dataclass
class MaintenanceTaskResult:
    """Execution result of a single maintenance task."""

    task_name: str
    status: str  # SUCCESS, SKIPPED, FAILED, DRY_RUN
    rows_affected: int = 0
    duration_seconds: float = 0.0
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert task result to dictionary."""
        return {
            "task_name": self.task_name,
            "status": self.status,
            "rows_affected": self.rows_affected,
            "duration_seconds": round(self.duration_seconds, 3),
            "error_message": self.error_message,
        }


@dataclass
class MaintenanceRunReport:
    """Aggregated report of a maintenance batch execution."""

    total_tasks: int
    successful_tasks: int
    failed_tasks: int
    total_rows_affected: int
    duration_seconds: float
    results: list[MaintenanceTaskResult] = field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert run report to dictionary."""
        return {
            "total_tasks": self.total_tasks,
            "successful_tasks": self.successful_tasks,
            "failed_tasks": self.failed_tasks,
            "total_rows_affected": self.total_rows_affected,
            "duration_seconds": round(self.duration_seconds, 3),
            "results": [r.to_dict() for r in self.results],
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }
