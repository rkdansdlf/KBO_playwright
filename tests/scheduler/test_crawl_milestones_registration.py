"""Tests for crawl_milestones job registration in the scheduler."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from src.scheduler.jobs.daily import crawl_milestones_job
from src.scheduler.registry import job_specs


def test_crawl_milestones_job_registered_in_job_specs() -> None:
    """crawl_milestones is registered with hour=6, minute=40 and runs before recalc_milestones_and_rag."""
    specs = {job_id: (fn, trigger, name, grace) for fn, trigger, job_id, name, grace in job_specs(CronTrigger)}

    assert "crawl_milestones" in specs
    fn, trigger, name, grace = specs["crawl_milestones"]
    assert fn is crawl_milestones_job
    assert "hour='6'" in str(trigger)
    assert "minute='40'" in str(trigger)

    # Verify crawl_milestones fires before recalc_milestones_and_rag
    assert "recalc_milestones_and_rag" in specs
    recalc_trigger = specs["recalc_milestones_and_rag"][1]

    ref_dt = datetime(2026, 4, 1, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    crawl_fire_time = trigger.get_next_fire_time(None, ref_dt)
    recalc_fire_time = recalc_trigger.get_next_fire_time(None, ref_dt)

    assert crawl_fire_time is not None
    assert recalc_fire_time is not None
    assert crawl_fire_time < recalc_fire_time
