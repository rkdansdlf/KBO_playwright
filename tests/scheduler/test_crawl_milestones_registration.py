"""Tests for crawl_milestones job registration in the scheduler."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from apscheduler.triggers.cron import CronTrigger

from src.scheduler.config import MILESTONE_CRAWL_ENABLED_ENV
from src.scheduler.jobs.daily import crawl_milestones_job
from src.scheduler.registry import job_specs


def test_crawl_milestones_absent_when_flag_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """crawl_milestones is absent from job_specs when the flag is unset."""
    monkeypatch.delenv(MILESTONE_CRAWL_ENABLED_ENV, raising=False)
    specs = {job_id: (fn, trigger, name, grace) for fn, trigger, job_id, name, grace in job_specs(CronTrigger)}
    assert "crawl_milestones" not in specs


@pytest.mark.parametrize("falsy_val", ["0", "false", "", "  ", "enabled"])
def test_crawl_milestones_absent_when_flag_falsy(monkeypatch: pytest.MonkeyPatch, falsy_val: str) -> None:
    """crawl_milestones is absent from job_specs when MILESTONE_CRAWL_ENABLED is 0 or false."""
    monkeypatch.setenv(MILESTONE_CRAWL_ENABLED_ENV, falsy_val)
    specs = {job_id: (fn, trigger, name, grace) for fn, trigger, job_id, name, grace in job_specs(CronTrigger)}
    assert "crawl_milestones" not in specs


def test_crawl_milestones_job_registered_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """crawl_milestones is present at 06:40 KST and fires before recalc when flag is 1."""
    monkeypatch.setenv(MILESTONE_CRAWL_ENABLED_ENV, "1")
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


@pytest.mark.parametrize("falsy_val", [None, "0", "false", ""])
def test_crawl_milestones_job_disabled_returns_early(
    monkeypatch: pytest.MonkeyPatch,
    falsy_val: str | None,
) -> None:
    """crawl_milestones_job returns early when disabled without importing or calling main."""
    if falsy_val is None:
        monkeypatch.delenv(MILESTONE_CRAWL_ENABLED_ENV, raising=False)
    else:
        monkeypatch.setenv(MILESTONE_CRAWL_ENABLED_ENV, falsy_val)

    # Patch DB gate to be hermetic without touching database
    monkeypatch.setattr("src.scheduler.locks._db_gate", lambda *args, **kwargs: True)

    import src.cli.collection.crawl_milestones

    def _exploding_main(*args: object, **kwargs: object) -> None:
        raise AssertionError("src.cli.collection.crawl_milestones.main must not be called when disabled")

    monkeypatch.setattr(src.cli.collection.crawl_milestones, "main", _exploding_main)

    # Must return cleanly
    crawl_milestones_job()
