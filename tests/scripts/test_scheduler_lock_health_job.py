"""Tests for the scheduler lock-health check job (scripts/scheduler.py)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "scheduler.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("scheduler_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_lock_health_check_job_passes_without_alert():
    sched = _load_module()
    result = MagicMock()
    result.returncode = 0
    result.stdout = "[OK] Scheduler lock health check passed"
    result.stderr = ""
    with patch.object(sched, "subprocess") as sp, patch.object(sched, "alert_warning") as warn:
        sp.run.return_value = result
        sched.lock_health_check_job()
        warn.assert_not_called()
        assert sp.run.called


def test_lock_health_check_job_failure_alerts():
    sched = _load_module()
    result = MagicMock()
    result.returncode = 1
    result.stdout = ""
    result.stderr = "LockAcquisitionError"
    with patch.object(sched, "subprocess") as sp, patch.object(sched, "alert_warning") as warn:
        sp.run.return_value = result
        sched.lock_health_check_job()
        warn.assert_called_once()
        assert "lock_health_check" in warn.call_args.args[0]


def test_lock_health_check_job_launch_failure_alerts():
    sched = _load_module()
    with patch.object(sched, "subprocess") as sp, patch.object(sched, "alert_warning") as warn:
        sp.run.side_effect = OSError("boom")
        sched.lock_health_check_job()
        warn.assert_called_once()
        assert "lock_health_check" in warn.call_args.args[0]


def test_p1p2_run_marker_writer_is_callable():
    sched = _load_module()
    assert callable(sched._write_p1p2_run_marker)
