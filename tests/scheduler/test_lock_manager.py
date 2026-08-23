"""Unit tests for src.scheduler.lock_manager."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from src.scheduler.dto import JobTier
from src.scheduler.lock_manager import SchedulerLockManager

if TYPE_CHECKING:
    from pathlib import Path


def test_lock_manager_tier_locks(tmp_path: Path) -> None:
    manager = SchedulerLockManager(lock_dir=tmp_path)
    assert manager.get_tier_lock(JobTier.LIVE) == manager.live_lock
    assert manager.get_tier_lock(JobTier.DAILY) == manager.daily_lock
    assert manager.get_tier_lock(JobTier.MAINTENANCE) == manager.maintenance_lock
    assert manager.get_tier_lock("sqlite_writer") == manager.sqlite_write_lock


def test_lock_manager_pid_lifecycle(tmp_path: Path) -> None:
    manager = SchedulerLockManager(lock_dir=tmp_path)
    assert manager.get_current_pid() is None
    assert manager.is_daemon_alive() is False

    # Write current PID
    manager.pid_file.write_text(str(os.getpid()))
    assert manager.get_current_pid() == os.getpid()
    assert manager.is_daemon_alive() is True

    # Release
    manager.pid_file.unlink(missing_ok=True)
    assert manager.get_current_pid() is None


def test_lock_manager_diagnose_locks(tmp_path: Path) -> None:
    manager = SchedulerLockManager(lock_dir=tmp_path)

    # Create dummy lock file with dead PID
    dead_lock = tmp_path / "test_dead.lock"
    dead_lock.write_text("99999999")

    report = manager.diagnose_locks()
    assert report.stale_locks_cleared >= 1
    assert not dead_lock.exists()
