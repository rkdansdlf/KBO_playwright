"""Tests for scripts/verify_p1p2_lock_fix.py (lock-fix contention logic)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

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
