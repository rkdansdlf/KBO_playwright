"""Scheduler Orchestrator managing APScheduler lifecycle and job dispatching."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import TYPE_CHECKING

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.scheduler.config import KST
from src.scheduler.dto import (
    JobExecutionRecord,
    ScheduledJobMeta,
    SchedulerHealthSummary,
)
from src.scheduler.lock_manager import SchedulerLockManager

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


CRONTAB_FIELDS_COUNT = 5


def build_cron_trigger(expression: str) -> CronTrigger:
    """Build a CronTrigger from standard 5-part crontab string or hour:minute."""
    parts = expression.strip().split()
    if len(parts) == CRONTAB_FIELDS_COUNT:
        return CronTrigger.from_crontab(expression, timezone=KST)
    if ":" in expression:
        h, m = expression.split(":")
        return CronTrigger(hour=int(h), minute=int(m), timezone=KST)
    return CronTrigger.from_crontab(expression, timezone=KST)


class SchedulerOrchestrator:
    """Coordinates scheduler execution, job registration, and runtime health monitoring."""

    def __init__(
        self,
        lock_manager: SchedulerLockManager | None = None,
        *,
        background: bool = False,
    ) -> None:
        """Initialize the scheduler orchestrator."""
        self.lock_manager = lock_manager or SchedulerLockManager()
        self.background = background
        self.scheduler = BackgroundScheduler(timezone=KST) if background else BlockingScheduler(timezone=KST)
        self.history: list[JobExecutionRecord] = []
        self._start_time = time.monotonic()
        self._registered_jobs: dict[str, ScheduledJobMeta] = {}

    def register_job(
        self,
        meta: ScheduledJobMeta,
        func: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> None:
        """Register a scheduled job with cron trigger and metadata."""
        self._registered_jobs[meta.job_id] = meta
        trigger = build_cron_trigger(meta.cron_expression)
        self.scheduler.add_job(
            func,
            trigger=trigger,
            id=meta.job_id,
            name=meta.name,
            max_instances=meta.max_instances,
            misfire_grace_time=meta.misfire_grace_time_seconds,
            args=args,
            kwargs=kwargs,
            replace_existing=True,
        )
        logger.info("Registered job '%s' (%s) with cron [%s]", meta.name, meta.job_id, meta.cron_expression)

    def dispatch_job(
        self,
        job_id: str,
        job_func: Callable[..., object],
        *args: object,
        **kwargs: object,
    ) -> JobExecutionRecord:
        """Manually execute a single job and record execution metrics."""
        started_dt = datetime.now(KST).isoformat()
        start_mono = time.monotonic()
        meta = self._registered_jobs.get(job_id)
        lock_name = meta.tier.value if meta else None

        try:
            logger.info("Dispatching job '%s'...", job_id)
            job_func(*args, **kwargs)
            duration = time.monotonic() - start_mono
            completed_dt = datetime.now(KST).isoformat()
            record = JobExecutionRecord(
                job_id=job_id,
                status="SUCCESS",
                started_at=started_dt,
                completed_at=completed_dt,
                duration_seconds=duration,
                lock_used=lock_name,
            )
        except Exception as exc:
            duration = time.monotonic() - start_mono
            completed_dt = datetime.now(KST).isoformat()
            logger.exception("Job '%s' failed after %.2fs", job_id, duration)
            record = JobExecutionRecord(
                job_id=job_id,
                status="FAILED",
                started_at=started_dt,
                completed_at=completed_dt,
                duration_seconds=duration,
                lock_used=lock_name,
                error_message=str(exc),
            )

        self.history.append(record)
        return record

    def get_health_summary(self) -> SchedulerHealthSummary:
        """Generate real-time health and execution summary of the scheduler daemon."""
        uptime = time.monotonic() - self._start_time
        pid = self.lock_manager.get_current_pid()
        is_alive = self.lock_manager.is_daemon_alive()
        lock_report = self.lock_manager.diagnose_locks()

        total_runs = len(self.history)
        successful_runs = sum(1 for r in self.history if r.status == "SUCCESS")
        failed_runs = sum(1 for r in self.history if r.status == "FAILED")

        return SchedulerHealthSummary(
            daemon_pid=pid or os.getpid(),
            is_alive=is_alive,
            uptime_seconds=uptime,
            active_jobs_count=len(self.scheduler.get_jobs()) if self.scheduler.running else len(self._registered_jobs),
            total_runs=total_runs,
            successful_runs=successful_runs,
            failed_runs=failed_runs,
            lock_report=lock_report,
        )

    def start(self) -> None:
        """Start the scheduler execution loop."""
        self.lock_manager.ensure_single_instance()
        logger.info("Starting scheduler (%s mode)...", "background" if self.background else "blocking")
        self.scheduler.start()

    def shutdown(self) -> None:
        """Gracefully shut down the scheduler and release locks."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        self.lock_manager.release_pid()
        logger.info("Scheduler shutdown complete.")
