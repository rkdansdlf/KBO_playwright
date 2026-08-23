"""Unit tests for src.scheduler.orchestrator."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from src.scheduler.dto import JobTier, ScheduledJobMeta
from src.scheduler.lock_manager import SchedulerLockManager
from src.scheduler.orchestrator import SchedulerOrchestrator

if TYPE_CHECKING:
    from pathlib import Path


def test_orchestrator_job_dispatch(tmp_path: Path) -> None:
    lock_mgr = SchedulerLockManager(lock_dir=tmp_path)
    orchestrator = SchedulerOrchestrator(lock_manager=lock_mgr, background=True)

    dummy_func = MagicMock()
    record = orchestrator.dispatch_job("test_job", dummy_func, "arg1", kw="val")

    assert record.job_id == "test_job"
    assert record.status == "SUCCESS"
    assert record.duration_seconds >= 0.0
    dummy_func.assert_called_once_with("arg1", kw="val")


def test_orchestrator_job_dispatch_failure(tmp_path: Path) -> None:
    lock_mgr = SchedulerLockManager(lock_dir=tmp_path)
    orchestrator = SchedulerOrchestrator(lock_manager=lock_mgr, background=True)

    def failing_func() -> None:
        raise ValueError("Job crashed")

    record = orchestrator.dispatch_job("failing_job", failing_func)

    assert record.job_id == "failing_job"
    assert record.status == "FAILED"
    assert record.error_message == "Job crashed"


def test_orchestrator_health_summary(tmp_path: Path) -> None:
    lock_mgr = SchedulerLockManager(lock_dir=tmp_path)
    orchestrator = SchedulerOrchestrator(lock_manager=lock_mgr, background=True)

    meta = ScheduledJobMeta(
        job_id="job1",
        tier=JobTier.DAILY,
        cron_expression="0 3 * * *",
        name="Job 1",
    )
    orchestrator.register_job(meta, lambda: None)

    summary = orchestrator.get_health_summary()
    assert summary.active_jobs_count >= 1
    assert summary.total_runs == 0
