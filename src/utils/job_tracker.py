"""Background job status tracker utility."""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class JobTracker:
    """In-memory tracker for async background tasks."""

    def __init__(self, max_history: int = 100) -> None:
        """Initialize the tracker with a bounded in-memory job history.

        Args:
            max_history: Maximum number of tracked jobs before eviction.

        """
        self._jobs: dict[str, dict[str, Any]] = {}
        self.max_history = max_history

    def create_job(self, job_type: str) -> str:
        """Create and register a new running job, returning its job id."""
        job_id = f"{job_type}_{int(time.time() * 1000)}"
        if len(self._jobs) >= self.max_history:
            oldest_key = next(iter(self._jobs))
            self._jobs.pop(oldest_key, None)

        self._jobs[job_id] = {
            "job_id": job_id,
            "job_type": job_type,
            "status": "running",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "completed_at": None,
            "error": None,
            "result": None,
        }
        return job_id

    def complete_job(self, job_id: str, result: dict[str, Any] | None = None) -> None:
        """Mark a tracked job as completed with an optional result payload."""
        if job_id in self._jobs:
            self._jobs[job_id]["status"] = "completed"
            self._jobs[job_id]["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            if result:
                self._jobs[job_id]["result"] = result

    def fail_job(self, job_id: str, error: str) -> None:
        """Mark a tracked job as failed with an error message."""
        if job_id in self._jobs:
            self._jobs[job_id]["status"] = "failed"
            self._jobs[job_id]["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            self._jobs[job_id]["error"] = error

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Return the tracked job state for a job id, or None when unknown."""
        return self._jobs.get(job_id)


job_tracker = JobTracker()
