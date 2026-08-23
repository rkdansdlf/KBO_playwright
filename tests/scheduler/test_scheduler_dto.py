"""Unit tests for src.scheduler.dto."""

from __future__ import annotations

from src.scheduler.dto import (
    JobExecutionRecord,
    JobTier,
    LockStatusReport,
    ScheduledJobMeta,
    SchedulerHealthSummary,
)


def test_job_tier_values() -> None:
    assert JobTier.LIVE == "live"
    assert JobTier.DAILY == "daily"
    assert JobTier.MAINTENANCE == "maintenance"
    assert JobTier.SENTINEL == "sentinel"
    assert JobTier.STADIUM == "stadium"


def test_scheduled_job_meta_to_dict() -> None:
    meta = ScheduledJobMeta(
        job_id="crawl_daily_games",
        tier=JobTier.DAILY,
        cron_expression="0 3 * * *",
        name="Daily Games Crawl",
        description="Daily games batch update",
    )
    d = meta.to_dict()
    assert d["job_id"] == "crawl_daily_games"
    assert d["tier"] == "daily"
    assert d["cron_expression"] == "0 3 * * *"


def test_job_execution_record_to_dict() -> None:
    record = JobExecutionRecord(
        job_id="live_refresh",
        status="SUCCESS",
        started_at="2026-08-24T18:30:00",
        completed_at="2026-08-24T18:30:05",
        duration_seconds=5.123,
        lock_used="live",
    )
    d = record.to_dict()
    assert d["job_id"] == "live_refresh"
    assert d["status"] == "SUCCESS"
    assert d["duration_seconds"] == 5.123
    assert d["lock_used"] == "live"


def test_lock_status_report_to_dict() -> None:
    report = LockStatusReport(
        daemon_pid=12345,
        pid_alive=True,
        active_locks={"live": {"status": "ACTIVE"}},
        stale_locks_cleared=1,
        skip_counts={"job_1:lock": 0},
    )
    d = report.to_dict()
    assert d["daemon_pid"] == 12345
    assert d["pid_alive"] is True
    assert d["stale_locks_cleared"] == 1


def test_scheduler_health_summary_to_dict() -> None:
    summary = SchedulerHealthSummary(
        daemon_pid=12345,
        is_alive=True,
        uptime_seconds=3600.5,
        active_jobs_count=15,
        total_runs=100,
        successful_runs=98,
        failed_runs=2,
    )
    d = summary.to_dict()
    assert d["daemon_pid"] == 12345
    assert d["active_jobs_count"] == 15
    assert d["successful_runs"] == 98
