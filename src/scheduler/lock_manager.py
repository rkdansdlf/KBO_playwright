"""Scheduler Lock Manager orchestrating multi-tier ProcessLocks and PID guards."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.scheduler.config import PROJECT_ROOT
from src.scheduler.dto import JobTier, LockStatusReport
from src.scheduler.locks import (
    _LAST_LOCK_SKIP,
    DAILY_LOCK,
    LIVE_LOCK,
    MAINTENANCE_LOCK,
    SQLITE_WRITE_LOCK,
    _ensure_single_scheduler_instance,
    _release_scheduler_pid_file,
    _scheduler_pid_alive,
)

if TYPE_CHECKING:
    from pathlib import Path

    from src.utils.lock import ProcessLock

logger = logging.getLogger(__name__)


class SchedulerLockManager:
    """Manages scheduler tier locks, process locking, and diagnostic reports."""

    def __init__(self, lock_dir: Path | None = None) -> None:
        """Initialize the lock manager with lock directory path."""
        self.lock_dir = lock_dir or (PROJECT_ROOT / "data" / "locks")
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        self.pid_file = self.lock_dir / "scheduler.pid"

    @property
    def live_lock(self) -> ProcessLock:
        """Return the LIVE tier ProcessLock."""
        return LIVE_LOCK

    @property
    def daily_lock(self) -> ProcessLock:
        """Return the DAILY tier ForceProcessLock."""
        return DAILY_LOCK

    @property
    def maintenance_lock(self) -> ProcessLock:
        """Return the MAINTENANCE tier ForceProcessLock."""
        return MAINTENANCE_LOCK

    @property
    def sqlite_write_lock(self) -> ProcessLock:
        """Return the SQLITE_WRITE tier ForceProcessLock."""
        return SQLITE_WRITE_LOCK

    def get_tier_lock(self, tier: JobTier | str) -> ProcessLock:
        """Get the corresponding ProcessLock for a given tier."""
        tier_val = tier.value if isinstance(tier, JobTier) else str(tier).lower()
        if tier_val == JobTier.LIVE.value:
            return self.live_lock
        if tier_val == JobTier.DAILY.value:
            return self.daily_lock
        if tier_val == JobTier.MAINTENANCE.value:
            return self.maintenance_lock
        if tier_val == "sqlite_writer":
            return self.sqlite_write_lock
        # Default to daily lock for safety
        return self.daily_lock

    def ensure_single_instance(self) -> None:
        """Ensure only a single scheduler instance runs by validating and creating PID file."""
        _ensure_single_scheduler_instance()

    def release_pid(self) -> None:
        """Release the scheduler PID file on shutdown."""
        _release_scheduler_pid_file()

    def get_current_pid(self) -> int | None:
        """Read the PID stored in the scheduler PID file."""
        if not self.pid_file.exists():
            return None
        try:
            content = self.pid_file.read_text().strip().split("\n")[0]
            return int(content) if content.isdigit() else None
        except OSError:
            return None

    def is_daemon_alive(self) -> bool:
        """Check if the PID recorded in the scheduler PID file is actively running."""
        pid = self.get_current_pid()
        if not pid:
            return False
        return _scheduler_pid_alive(pid)

    def diagnose_locks(self) -> LockStatusReport:
        """Diagnose all tier lock files and return a comprehensive status report."""
        pid = self.get_current_pid()
        is_alive = self.is_daemon_alive()
        stale_cleared = 0
        active_locks: dict[str, dict[str, object]] = {}

        for lock_file in self.lock_dir.glob("*.lock"):
            name = lock_file.stem
            try:
                content = lock_file.read_text().strip()
                lock_pid = int(content) if content.isdigit() else None
                pid_running = _scheduler_pid_alive(lock_pid) if lock_pid else False

                if lock_pid and not pid_running:
                    lock_file.unlink(missing_ok=True)
                    stale_cleared += 1
                    active_locks[name] = {"pid": lock_pid, "status": "STALE_CLEARED"}
                else:
                    active_locks[name] = {"pid": lock_pid, "status": "ACTIVE" if pid_running else "HELD"}
            except OSError as exc:
                active_locks[name] = {"status": "ERROR", "error": str(exc)}

        skip_counts_formatted = {f"{k[0]}:{k[1]}": int(v) for k, v in _LAST_LOCK_SKIP.items()}

        return LockStatusReport(
            daemon_pid=pid,
            pid_alive=is_alive,
            active_locks=active_locks,
            stale_locks_cleared=stale_cleared,
            skip_counts=skip_counts_formatted,
        )
