"""Standard Data Transfer Objects (DTOs) for KBO Scheduler and Concurrency Locks."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class JobTier(StrEnum):
    """Execution tiers for scheduled jobs."""

    LIVE = "live"
    DAILY = "daily"
    MAINTENANCE = "maintenance"
    SENTINEL = "sentinel"
    STADIUM = "stadium"


@dataclass
class ScheduledJobMeta:
    """Metadata describing a scheduled job's cron schedule and configuration."""

    job_id: str
    tier: JobTier
    cron_expression: str
    name: str
    description: str = ""
    max_instances: int = 1
    misfire_grace_time_seconds: int = 300

    def to_dict(self) -> dict[str, Any]:
        """Convert scheduled job metadata to dictionary."""
        return {
            "job_id": self.job_id,
            "tier": self.tier.value,
            "cron_expression": self.cron_expression,
            "name": self.name,
            "description": self.description,
            "max_instances": self.max_instances,
            "misfire_grace_time_seconds": self.misfire_grace_time_seconds,
        }


@dataclass
class JobExecutionRecord:
    """Record of a single job execution."""

    job_id: str
    status: str  # SUCCESS, SKIPPED, FAILED
    started_at: str
    completed_at: str
    duration_seconds: float
    lock_used: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert execution record to dictionary."""
        return {
            "job_id": self.job_id,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": round(self.duration_seconds, 3),
            "lock_used": self.lock_used,
            "error_message": self.error_message,
        }


@dataclass
class LockStatusReport:
    """Diagnostic status report of scheduler process and tier locks."""

    daemon_pid: int | None
    pid_alive: bool
    active_locks: dict[str, Any] = field(default_factory=dict)
    stale_locks_cleared: int = 0
    skip_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert lock status report to dictionary."""
        return asdict(self)


@dataclass
class SchedulerHealthSummary:
    """Aggregated health status and uptime metrics for the scheduler daemon."""

    daemon_pid: int | None
    is_alive: bool
    uptime_seconds: float
    active_jobs_count: int
    total_runs: int
    successful_runs: int
    failed_runs: int
    lock_report: LockStatusReport | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert health summary to dictionary."""
        return {
            "daemon_pid": self.daemon_pid,
            "is_alive": self.is_alive,
            "uptime_seconds": round(self.uptime_seconds, 1),
            "active_jobs_count": self.active_jobs_count,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
            "lock_report": self.lock_report.to_dict() if self.lock_report else None,
        }
