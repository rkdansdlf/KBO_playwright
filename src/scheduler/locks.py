"""Concurrency control, process locking, and scheduler PID guards."""

from __future__ import annotations

import atexit
import contextlib
import functools
import logging
import os
import sys
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

from src.scheduler.config import (
    ALERT_EXCEPTIONS,
    LOCK_SKIP_ALERT_THRESHOLD,
    PROJECT_ROOT,
    SQLITE_WRITE_LOCK_TIMEOUT_SECONDS,
    _scheduler_uses_sqlite_database,
)
from src.utils.alerting import SlackWebhookClient
from src.utils.lock import ForceProcessLock, LockAcquisitionError, ProcessLock
from src.utils.metrics import KBO_SCHEDULER_LOCK_SKIP_TOTAL

logger = logging.getLogger("src.scheduler.locks")

# Single-instance guard: only one scheduler process may hold this PID file at a time.
_SCHEDULER_PID_FILE = PROJECT_ROOT / "data" / "locks" / "scheduler.pid"

# Granular locking to prevent long-running batch jobs from blocking real-time updates
LIVE_LOCK = ProcessLock("live_refresh")
DAILY_LOCK = ForceProcessLock("daily_update")
MAINTENANCE_LOCK = ForceProcessLock("maintenance")
SQLITE_WRITE_LOCK = ForceProcessLock("sqlite_writer")

# Last observed cumulative skip totals, keyed by (job_id, lock), for delta computation.
_LAST_LOCK_SKIP: dict[tuple[str, str], float] = {}


def _scheduler_pid_alive(pid: int) -> bool:
    """Return whether a scheduler PID is currently running."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        process_query_limited_information = 0x1000
        access_denied = 5
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.restype = ctypes.c_void_p
        inherit_handle = 0
        handle = kernel32.OpenProcess(process_query_limited_information, inherit_handle, pid)
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return ctypes.get_last_error() == access_denied
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _get_scheduler_pid_file() -> Path:
    default_path = PROJECT_ROOT / "data" / "locks" / "scheduler.pid"
    if default_path != _SCHEDULER_PID_FILE:
        return _SCHEDULER_PID_FILE
    mod = sys.modules.get("scripts.scheduler")
    if mod and hasattr(mod, "_SCHEDULER_PID_FILE") and default_path != mod._SCHEDULER_PID_FILE:  # noqa: SLF001
        return mod._SCHEDULER_PID_FILE  # noqa: SLF001
    return _SCHEDULER_PID_FILE


def _ensure_single_scheduler_instance() -> None:
    """Exit if another live scheduler process already holds the PID file.

    Stale PID files (process no longer running) are cleared automatically.
    """
    pid_file = _get_scheduler_pid_file()

    try:
        if pid_file.exists():
            stale = False
            pid_str = "unknown"
            try:
                pid_str = pid_file.read_text().strip().split("\n")[0]
                if pid_str.isdigit():
                    pid = int(pid_str)
                    stale = True if pid == os.getpid() else not _scheduler_pid_alive(pid)
                else:
                    stale = True
            except OSError:
                stale = True
            if stale:
                logger.warning("Removing stale scheduler PID file")
                pid_file.unlink(missing_ok=True)
            else:
                logger.error(
                    "Another scheduler instance (PID %s) is already running. Exiting to avoid lock contention.",
                    pid_str,
                )
                sys.exit(1)
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(f"{os.getpid()}\n")
        atexit.register(_release_scheduler_pid_file)
    except OSError as e:
        logger.warning("Could not set up scheduler PID file guard: %s", e)


def _release_scheduler_pid_file() -> None:
    """Remove the scheduler PID file on clean shutdown."""
    pid_file = _get_scheduler_pid_file()

    try:
        if pid_file.exists():
            content = pid_file.read_text().strip().split("\n")[0]
            if content == str(os.getpid()):
                pid_file.unlink(missing_ok=True)
    except OSError:
        pass


@contextmanager
def _sqlite_writer_lock(
    *,
    blocking: bool = True,
    timeout: float | None = None,
    job_id: str = "unknown",
) -> Iterator[bool]:
    """Guard SQLite writes with the shared ``sqlite_writer`` lock.

    Yields ``True`` when the caller may proceed (PostgreSQL/Oracle backend, or the
    SQLite writer lock was acquired). Yields ``False`` when running on SQLite
    and the lock could not be acquired, signalling the caller to skip the write
    and let a later cycle retry.
    """
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    sqlite_lock = getattr(mod, "SQLITE_WRITE_LOCK", SQLITE_WRITE_LOCK) if mod else SQLITE_WRITE_LOCK
    if not _scheduler_uses_sqlite_database():
        yield True
        return

    if not sqlite_lock.acquire(blocking=blocking, timeout=timeout):
        lock_name = getattr(sqlite_lock, "name", "sqlite_writer")
        logger.info(
            "Skipping SQLite write: %s lock is held by another job",
            lock_name,
        )
        counter = (
            getattr(mod, "KBO_SCHEDULER_LOCK_SKIP_TOTAL", KBO_SCHEDULER_LOCK_SKIP_TOTAL)
            if mod
            else KBO_SCHEDULER_LOCK_SKIP_TOTAL
        )
        with contextlib.suppress(AttributeError, TypeError):
            counter.labels(job_id=job_id, lock="sqlite_writer").inc()
        yield False
        return

    try:
        yield True
    finally:
        sqlite_lock.release()


class _LockSkipped(Exception):  # noqa: N818
    """Internal control-flow signal: a scheduler job was skipped due to a lock timeout."""


def _with_lock_skip_guard(func: Callable[..., object]) -> Callable[..., object]:
    """Catch ``_LockSkipped`` and log a clean warning."""

    @functools.wraps(func)  # type: ignore[arg-type]
    def wrapper(*args: object, **kwargs: object) -> object:
        try:
            return func(*args, **kwargs)  # type: ignore[operator]
        except _LockSkipped:
            logger.warning("Job %s skipped: sqlite_writer lock timed out", getattr(func, "__name__", "unknown"))
            return None

    return wrapper


@contextmanager
def _scheduler_job_lock(
    tier_lock: ProcessLock | ForceProcessLock,
    *,
    lock_timeout: float | None = None,
    sqlite_timeout: float | None = None,
) -> Iterator[None]:
    """Acquire tier lock, and on SQLite additionally acquire the writer lock.

    When running against a SQLite database, tier-locked jobs (daily batch,
    maintenance) also acquire ``SQLITE_WRITE_LOCK`` to serialize with other
    writers and prevent SQLITE_BUSY deadlocks.

    If the SQLite writer lock cannot be acquired within
    ``SQLITE_WRITE_LOCK_TIMEOUT_SECONDS``, the job raises ``_LockSkipped`` so
    it can release the tier lock, log the skip, and let the scheduler retry on
    the next cycle.
    """
    timeout = lock_timeout if lock_timeout is not None else SQLITE_WRITE_LOCK_TIMEOUT_SECONDS
    sq_timeout = sqlite_timeout if sqlite_timeout is not None else SQLITE_WRITE_LOCK_TIMEOUT_SECONDS
    try:
        acquired = tier_lock.acquire(blocking=True, timeout=timeout)
    except LockAcquisitionError:
        acquired = False
    if not acquired:
        logger.warning(
            "[%s] Could not acquire tier lock within %ss; skipping job",
            getattr(tier_lock, "name", "tier"),
            timeout,
        )
        raise _LockSkipped
    try:
        if not _scheduler_uses_sqlite_database():
            yield
            return

        with _sqlite_writer_lock(timeout=sq_timeout) as sq_acquired:
            if not sq_acquired:
                logger.warning(
                    "[%s] Could not acquire sqlite_writer lock within %ss; skipping job",
                    getattr(tier_lock, "name", "tier"),
                    sq_timeout,
                )
                raise _LockSkipped
            yield
    finally:
        tier_lock.release()


def lock_skip_monitor_job() -> None:
    """Monitor lock skip rate and alert via Slack when threshold is exceeded."""
    logger.info("=== Checking Scheduler Lock Skip Rate ===")
    mod = sys.modules.get("scripts.scheduler") or sys.modules.get("src.scheduler")
    last_skips = getattr(mod, "_LAST_LOCK_SKIP", _LAST_LOCK_SKIP) if mod else _LAST_LOCK_SKIP
    threshold = (
        getattr(mod, "LOCK_SKIP_ALERT_THRESHOLD", LOCK_SKIP_ALERT_THRESHOLD) if mod else LOCK_SKIP_ALERT_THRESHOLD
    )
    counter = (
        getattr(mod, "KBO_SCHEDULER_LOCK_SKIP_TOTAL", KBO_SCHEDULER_LOCK_SKIP_TOTAL)
        if mod
        else KBO_SCHEDULER_LOCK_SKIP_TOTAL
    )
    slack_client = getattr(mod, "SlackWebhookClient", SlackWebhookClient) if mod else SlackWebhookClient

    try:
        try:
            metrics = counter.collect()
        except (AttributeError, RuntimeError, OSError):
            logger.info("Lock skip metric not registered yet; skipping check")
            return
        if not metrics:
            return
        alerts: list[str] = []
        for metric in metrics:
            for sample in getattr(metric, "samples", []):
                if sample.name != "kbo_scheduler_lock_skip_total":
                    continue
                job_id = sample.labels.get("job_id", "unknown")
                lock_name = sample.labels.get("lock", "unknown")
                key = (job_id, lock_name)
                current_value = sample.value
                prev_value = last_skips.get(key, 0.0)
                delta = current_value - prev_value
                last_skips[key] = current_value
                if delta >= threshold:
                    msg = f"• Job <code>{job_id}</code> skipped {int(delta)} times on lock <code>{lock_name}</code>"
                    alerts.append(msg)
                    logger.warning(
                        "[LockSkipAlert] High lock contention: job=%s lock=%s skips_in_interval=%d (threshold=%d)",
                        job_id,
                        lock_name,
                        int(delta),
                        threshold,
                    )
        if alerts:
            details = f"High lock contention detected in the last 15 minutes (threshold: {threshold}):\n" + "\n".join(
                alerts
            )
            slack_client.send_alert(
                f"⚠️ <b>Scheduler Lock Skip Alert</b>\n{details}",
            )
        else:
            logger.info("=== Lock Skip Rate Check Passed (no excessive skips) ===")
    except ALERT_EXCEPTIONS:
        logger.exception("Error during lock skip monitor check")
