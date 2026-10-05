"""Tests for the canonical scheduler lock-health check job.

The job lives in ``src/scheduler/jobs/daily.py``; ``scripts/scheduler.py`` is only
a bootstrap re-export, so these tests target the canonical module and its own
dependency gate.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.scheduler.jobs.daily import (
    JobStatus,
    _register_job,
    _update_job_status,
    clear_job_registry,
    lock_health_check_job,
)


@pytest.fixture(autouse=True)
def _isolated_job_registry():
    clear_job_registry()
    try:
        yield
    finally:
        clear_job_registry()


def _satisfy_dependency() -> None:
    _register_job("crawl_p1p2_data_job", dependencies=[])
    _update_job_status("crawl_p1p2_data_job", JobStatus.SUCCESS, "test setup")


def test_lock_health_check_job_passes_without_alert() -> None:
    _satisfy_dependency()
    result = MagicMock(returncode=0, stdout="[OK] Scheduler lock health check passed", stderr="")

    with (
        patch("src.scheduler.jobs.daily.subprocess") as sp,
        patch("src.scheduler.jobs.daily.alert_warning") as warn,
        patch("src.scheduler.jobs.daily.alert_success") as success,
    ):
        sp.run.return_value = result
        lock_health_check_job()

    warn.assert_not_called()
    # The warning is conditional, so the clean run is what has to clear it:
    # nothing else owns ``scheduler:lock_health_check:warning``.
    assert success.call_args.args[0] == "lock_health_check"
    assert sp.run.called


def test_lock_health_check_job_failure_alerts() -> None:
    _satisfy_dependency()
    result = MagicMock(returncode=1, stdout="", stderr="LockAcquisitionError")

    with (
        patch("src.scheduler.jobs.daily.subprocess") as sp,
        patch("src.scheduler.jobs.daily.alert_warning") as warn,
        patch("src.scheduler.jobs.daily.alert_success") as success,
    ):
        sp.run.return_value = result
        lock_health_check_job()

    warn.assert_called_once()
    assert "lock_health_check" in warn.call_args.args[0]
    # A failing check must not also report recovery.
    success.assert_not_called()


def test_lock_health_check_job_launch_failure_alerts() -> None:
    _satisfy_dependency()

    with (
        patch("src.scheduler.jobs.daily.subprocess") as sp,
        patch("src.scheduler.jobs.daily.alert_warning") as warn,
        patch("src.scheduler.jobs.daily.alert_success") as success,
    ):
        sp.run.side_effect = OSError("boom")
        lock_health_check_job()

    warn.assert_called_once()
    assert "lock_health_check" in warn.call_args.args[0]
    success.assert_not_called()


def test_lock_health_check_job_skips_without_dependency() -> None:
    with (
        patch("src.scheduler.jobs.daily.subprocess") as sp,
        patch("src.scheduler.jobs.daily.alert_success") as success,
    ):
        lock_health_check_job()

    sp.run.assert_not_called()
    # A skip is not a recovery either: this run learned nothing about lock health.
    success.assert_not_called()


def test_p1p2_run_marker_writer_is_callable() -> None:
    from src.scheduler.jobs.daily import _write_p1p2_run_marker

    assert callable(_write_p1p2_run_marker)
