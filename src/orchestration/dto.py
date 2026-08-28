"""Standard Data Transfer Objects (DTOs) for Master Workflow DAG Orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class WorkflowStageType(StrEnum):
    """Functional stage types in a workflow DAG."""

    INGESTION = "ingestion"
    PROCESSING = "processing"
    ANALYTICS = "analytics"
    QUALITY_GATE = "quality_gate"
    SYNC = "sync"
    NOTIFICATION = "notification"


class StageExecutionStatus(StrEnum):
    """Lifecycle execution status of a single workflow stage."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"


@dataclass
class WorkflowStageMeta:
    """Metadata definition of a single workflow stage in a DAG."""

    stage_id: str
    stage_name: str
    stage_type: WorkflowStageType
    depends_on: list[str] = field(default_factory=list)
    timeout_seconds: int = 300
    retry_limit: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert stage metadata to dictionary."""
        return {
            "stage_id": self.stage_id,
            "stage_name": self.stage_name,
            "stage_type": self.stage_type.value,
            "depends_on": self.depends_on,
            "timeout_seconds": self.timeout_seconds,
            "retry_limit": self.retry_limit,
        }


@dataclass
class StageExecutionResult:
    """Execution outcome of a single workflow stage."""

    stage_id: str
    status: StageExecutionStatus
    duration_seconds: float = 0.0
    records_processed: int = 0
    error_message: str | None = None
    artifacts: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert stage result to dictionary."""
        return {
            "stage_id": self.stage_id,
            "status": self.status.value,
            "duration_seconds": round(self.duration_seconds, 3),
            "records_processed": self.records_processed,
            "error_message": self.error_message,
            "artifacts": self.artifacts,
        }


@dataclass
class MasterWorkflowRunReport:
    """Aggregated execution report of an entire master workflow DAG."""

    workflow_id: str
    total_stages: int
    completed_stages: int
    failed_stages: int
    skipped_stages: int
    duration_seconds: float
    stage_results: list[StageExecutionResult] = field(default_factory=list)
    overall_status: str = "SUCCESS"  # SUCCESS, PARTIAL_FAILURE, FAILED
    started_at: str = ""
    completed_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert workflow run report to dictionary."""
        return {
            "workflow_id": self.workflow_id,
            "total_stages": self.total_stages,
            "completed_stages": self.completed_stages,
            "failed_stages": self.failed_stages,
            "skipped_stages": self.skipped_stages,
            "duration_seconds": round(self.duration_seconds, 3),
            "stage_results": [r.to_dict() for r in self.stage_results],
            "overall_status": self.overall_status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }
