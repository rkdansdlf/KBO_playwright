"""Unit tests for src/scheduler/locks.py."""

from __future__ import annotations

import os
from unittest.mock import patch

from src.scheduler.locks import (
    DAILY_LOCK,
    LIVE_LOCK,
    MAINTENANCE_LOCK,
    SQLITE_WRITE_LOCK,
    _LockSkipped,
    _ensure_single_scheduler_instance,
    _release_scheduler_pid_file,
    _scheduler_job_lock,
    _scheduler_pid_alive,
    _sqlite_writer_lock,
    _with_lock_skip_guard,
)


def test_scheduler_pid_alive():
    assert _scheduler_pid_alive(0) is False
    assert _scheduler_pid_alive(-1) is False
    assert _scheduler_pid_alive(os.getpid()) is True


def test_ensure_single_scheduler_instance(tmp_path):
    pid_file = tmp_path / "scheduler.pid"
    with patch("src.scheduler.locks._SCHEDULER_PID_FILE", pid_file):
        _ensure_single_scheduler_instance()
        assert pid_file.exists()
        assert pid_file.read_text().strip() == str(os.getpid())
        _release_scheduler_pid_file()
        assert not pid_file.exists()


def test_sqlite_writer_lock_oracle_bypass():
    with patch("src.scheduler.locks._scheduler_uses_sqlite_database", return_value=False):
        with _sqlite_writer_lock() as acquired:
            assert acquired is True


def test_with_lock_skip_guard():
    @_with_lock_skip_guard
    def failing_job():
        raise _LockSkipped

    @_with_lock_skip_guard
    def succeeding_job():
        return 42

    assert failing_job() is None
    assert succeeding_job() == 42


def test_scheduler_job_lock_oracle_mode():
    with patch("src.scheduler.locks._scheduler_uses_sqlite_database", return_value=False):
        with _scheduler_job_lock(DAILY_LOCK):
            pass


def test_locks_singleton_names():
    assert LIVE_LOCK.name == "live_refresh"
    assert DAILY_LOCK.name == "daily_update"
    assert MAINTENANCE_LOCK.name == "maintenance"
    assert SQLITE_WRITE_LOCK.name == "sqlite_writer"
