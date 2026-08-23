"""Standard Data Transfer Objects (DTOs) for GitHub Actions CI/CD Workflow Auditing."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class WorkflowTriggerType(StrEnum):
    """Trigger event types for GitHub Actions workflows."""

    SCHEDULE = "schedule"
    WORKFLOW_DISPATCH = "workflow_dispatch"
    PUSH = "push"
    PULL_REQUEST = "pull_request"
    OTHER = "other"


@dataclass
class WorkflowJobMeta:
    """Metadata describing a single job in a workflow."""

    job_id: str
    name: str = ""
    runs_on: str = "ubuntu-latest"
    timeout_minutes: int | None = None
    uses_composite_action: bool = False
    composite_actions_used: list[str] = field(default_factory=list)
    env_keys: list[str] = field(default_factory=list)
    secret_keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert job metadata to dictionary."""
        return asdict(self)


@dataclass
class WorkflowMeta:
    """Metadata describing a GitHub Actions workflow file."""

    file_path: str
    workflow_name: str
    triggers: list[WorkflowTriggerType] = field(default_factory=list)
    cron_schedules: list[str] = field(default_factory=list)
    jobs: list[WorkflowJobMeta] = field(default_factory=list)
    has_concurrency_guard: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert workflow metadata to dictionary."""
        return {
            "file_path": self.file_path,
            "workflow_name": self.workflow_name,
            "triggers": [t.value for t in self.triggers],
            "cron_schedules": self.cron_schedules,
            "jobs": [j.to_dict() for j in self.jobs],
            "has_concurrency_guard": self.has_concurrency_guard,
        }


@dataclass
class WorkflowAuditIssue:
    """Represents a static audit issue or defect detected in a workflow."""

    severity: str  # ERROR, WARN, INFO
    workflow_file: str
    rule_name: str
    message: str
    job_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert audit issue to dictionary."""
        return asdict(self)


@dataclass
class WorkflowAuditReport:
    """Aggregated audit report of all workflows in the repository."""

    total_workflows: int
    total_jobs: int
    passed_workflows: int
    failed_workflows: int
    issues: list[WorkflowAuditIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert audit report to dictionary."""
        return {
            "total_workflows": self.total_workflows,
            "total_jobs": self.total_jobs,
            "passed_workflows": self.passed_workflows,
            "failed_workflows": self.failed_workflows,
            "issues_count": len(self.issues),
            "issues": [i.to_dict() for i in self.issues],
        }
