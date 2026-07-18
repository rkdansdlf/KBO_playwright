from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scripts import scheduler


def test_stale_scheduler_pid_is_replaced_and_released(tmp_path, monkeypatch) -> None:
    pid_file = tmp_path / "scheduler.pid"
    pid_file.write_text("not-a-pid\n")
    monkeypatch.setattr(scheduler, "_SCHEDULER_PID_FILE", pid_file)

    scheduler._ensure_single_scheduler_instance()

    assert pid_file.read_text() == f"{os.getpid()}\n"
    scheduler._release_scheduler_pid_file()
    assert not pid_file.exists()


def test_live_scheduler_pid_aborts_without_replacing_file(tmp_path, monkeypatch) -> None:
    pid_file = tmp_path / "scheduler.pid"
    pid_file.write_text(f"{os.getpid()}\n")
    monkeypatch.setattr(scheduler, "_SCHEDULER_PID_FILE", pid_file)

    with pytest.raises(SystemExit, match="1"):
        scheduler._ensure_single_scheduler_instance()

    assert pid_file.read_text() == f"{os.getpid()}\n"


def test_scheduler_pid_release_keeps_another_process_file(tmp_path, monkeypatch) -> None:
    pid_file = tmp_path / "scheduler.pid"
    pid_file.write_text("999999\n")
    monkeypatch.setattr(scheduler, "_SCHEDULER_PID_FILE", pid_file)

    scheduler._release_scheduler_pid_file()

    assert pid_file.exists()


def test_dead_scheduler_pid_is_cleared_and_replaced(tmp_path, monkeypatch) -> None:
    """A pid file owned by a dead (non-running) numeric PID must be treated as stale.

    This is the real-world crash scenario: a scheduler process died without
    releasing the pid file, leaving a numeric PID that no longer maps to a live
    process. The guard must clear it and write the current PID.
    """
    pid_file = tmp_path / "scheduler.pid"
    # 999999 is overwhelmingly unlikely to be a live PID on the test host.
    pid_file.write_text("999999\n")
    monkeypatch.setattr(scheduler, "_SCHEDULER_PID_FILE", pid_file)

    scheduler._ensure_single_scheduler_instance()

    assert pid_file.read_text() == f"{os.getpid()}\n"
    scheduler._release_scheduler_pid_file()
    assert not pid_file.exists()


def test_lock_skip_monitor_handles_counter_reset(monkeypatch) -> None:
    key = ("crawl_congestion", "sqlite_writer")
    sample = SimpleNamespace(
        name="kbo_scheduler_lock_skip_total",
        labels={"job_id": key[0], "lock": key[1]},
        value=2.0,
    )
    counter = MagicMock()
    counter.collect.return_value = [SimpleNamespace(samples=[sample])]
    alert = MagicMock()
    monkeypatch.setattr(scheduler, "KBO_SCHEDULER_LOCK_SKIP_TOTAL", counter)
    monkeypatch.setattr(scheduler, "_LAST_LOCK_SKIP", {key: 10.0})
    monkeypatch.setattr(scheduler, "LOCK_SKIP_ALERT_THRESHOLD", 5.0)
    monkeypatch.setattr(scheduler.SlackWebhookClient, "send_alert", alert)

    scheduler.lock_skip_monitor_job()

    assert scheduler._LAST_LOCK_SKIP[key] == 2.0
    alert.assert_not_called()

    sample.value = 8.0
    scheduler.lock_skip_monitor_job()

    alert.assert_called_once()


def test_lock_skip_monitor_returns_when_metrics_collection_fails(monkeypatch) -> None:
    counter = MagicMock()
    counter.collect.side_effect = RuntimeError("metrics unavailable")
    monkeypatch.setattr(scheduler, "KBO_SCHEDULER_LOCK_SKIP_TOTAL", counter)
    alert = MagicMock()
    monkeypatch.setattr(scheduler.SlackWebhookClient, "send_alert", alert)

    scheduler.lock_skip_monitor_job()

    alert.assert_not_called()


def test_scheduler_job_lock_converts_tier_lock_error_to_skip() -> None:
    tier_lock = MagicMock()
    tier_lock.acquire.side_effect = scheduler.LockAcquisitionError("lock failed")

    with pytest.raises(scheduler._LockSkipped):
        with scheduler._scheduler_job_lock(tier_lock):
            pytest.fail("the tier lock should have skipped the job")

    tier_lock.release.assert_not_called()
