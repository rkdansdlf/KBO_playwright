"""Tests for scripts/verify_p1p2_lock_fix.py (lock-fix contention logic)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from src.crawlers.sweep_outcome import CrawlSweepOutcome
from src.models.crawl_execution import RUN_STATUS_SUCCESS

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify_p1p2_lock_fix.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("verify_p1p2_lock_fix", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_scenario_passes_when_job_succeeds():
    mod = _load_module()

    calls: list[str] = []

    def acquire():
        calls.append("acquire")
        return True

    def release():
        calls.append("release")

    def run_job():
        calls.append("run")

    rc = mod.run_contention_scenario(acquire, release, run_job)
    assert rc == 0
    assert calls == ["acquire", "run", "release"]


def test_scenario_fails_when_lock_acquisition_error():
    mod = _load_module()
    lock_error = mod.scheduler.LockAcquisitionError

    def acquire():
        return True

    def release():
        pass

    def run_job():
        raise lock_error("nested daily_update collision")

    assert mod.run_contention_scenario(acquire, release, run_job) == 1


def test_scenario_skips_when_acquire_fails():
    mod = _load_module()

    def acquire():
        return False

    def release():
        pass

    def run_job():
        raise AssertionError("should not be called when lock not acquired")

    assert mod.run_contention_scenario(acquire, release, run_job) == 2


def test_scenario_fails_on_unexpected_exception():
    mod = _load_module()
    lock_error = mod.scheduler.LockAcquisitionError

    def acquire():
        return True

    def release():
        pass

    def run_job():
        raise ValueError("unexpected")

    assert mod.run_contention_scenario(acquire, release, run_job) == 1
    # LockAcquisitionError class is referenced, not the value above.
    assert lock_error is not None


def test_the_stub_returns_an_outcome_because_the_job_reads_one():
    """The job aggregates what its children returned, so the stub returns that shape.

    `_noop_main` used to return `0`. Fed to the job's aggregation, a return code
    raises an `AttributeError` that this scenario then reports as a lock failure
    -- the one thing it exists to observe. The stub stands in for a crawler
    entrypoint, so it returns what one returns.
    """
    mod = _load_module()

    outcome = mod._noop_main(["--save"])

    assert isinstance(outcome, CrawlSweepOutcome)
    assert outcome.status == RUN_STATUS_SUCCESS
