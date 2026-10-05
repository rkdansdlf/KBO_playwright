"""Concurrency control, process locking, and scheduler PID guards."""

from __future__ import annotations

import atexit
import contextlib
import functools
import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

from src.db.engine import Engine, database_reachable
from src.scheduler.config import (
    ALERT_EXCEPTIONS,
    LOCK_SKIP_ALERT_THRESHOLD,
    PROJECT_ROOT,
    SQLITE_WRITE_LOCK_TIMEOUT_SECONDS,
    _scheduler_uses_sqlite_database,
)
from src.utils.lock import ForceProcessLock, LockAcquisitionError, ProcessLock
from src.utils.metrics import KBO_SCHEDULER_LOCK_SKIP_TOTAL

logger = logging.getLogger("src.scheduler.locks")

# Single-instance guard: only one scheduler process may hold this PID file at a time.
_SCHEDULER_PID_FILE = PROJECT_ROOT / "data" / "locks" / "scheduler.pid"

# Granular locking to prevent long-running batch jobs from blocking real-time updates
LIVE_LOCK = ForceProcessLock("live_refresh")
DAILY_LOCK = ForceProcessLock("daily_update")
MAINTENANCE_LOCK = ForceProcessLock("maintenance")
SQLITE_WRITE_LOCK = ForceProcessLock("sqlite_writer")

# Last observed cumulative skip totals, keyed by (job_id, lock), for delta computation.
_LAST_LOCK_SKIP: dict[tuple[str, str], float] = {}

#: Namespace for lock-contention incidents, used to reconcile on recovery.
LOCK_SKIP_KEY_PREFIX = "scheduler:lock_skip:"

#: How long a failed reachability probe is trusted before the next job re-checks.
#: Long enough that a dead database is discovered once per window rather than
#: once per job tick, short enough that recovery is not noticeably delayed.
DB_GATE_FAILURE_COOLDOWN_SECONDS = float(os.getenv("DB_GATE_FAILURE_COOLDOWN_SECONDS", "30"))


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


class _DbGate:
    """Memoises the reachability probe so an outage costs one query per window.

    The memo is keyed on the *set of URLs* asked about, not on a single result.
    A RAG build may need the operational database plus a vector store, and a
    memo shared between a one-database job and a two-database job would let the
    first job's answer stand in for the second's -- which is the blind spot the
    keyed entry exists to close.

    Mutable module state rather than a ``global`` pair: the value is read and
    written from two places, and threading them through call sites would only
    move the bug to whoever forgot to pass them.
    """

    def __init__(self, *, cooldown_seconds: float) -> None:
        """Initialize empty, so the first call for any URL set always probes."""
        self.cooldown_seconds = cooldown_seconds
        self._entries: dict[tuple[str, ...], tuple[float, bool]] = {}
        self.probe_count = 0

    def check(self, urls: tuple[str, ...]) -> bool:
        """Return whether every URL answers, probing at most once per window."""
        now = time.monotonic()
        cached = self._entries.get(urls)
        if cached is not None and now < cached[0]:
            return cached[1]
        self.probe_count += 1
        reachable = database_reachable(urls=urls)
        self._entries[urls] = (now if reachable else now + self.cooldown_seconds, reachable)
        return reachable

    def reset(self) -> None:
        """Forget every memoised result, so the next call probes again."""
        self._entries.clear()
        self.probe_count = 0


#: Shared gate. The cooldown is the only knob: an outage is discovered once per
#: window rather than once per job tick, and a recovery is picked up on the next
#: window because a successful result is never cached past it.
_DB_GATE = _DbGate(cooldown_seconds=DB_GATE_FAILURE_COOLDOWN_SECONDS)


#: Causes carried by ``_LockSkipped``. Named rather than free text so the two
#: raise sites, the guard's message and the tests cannot drift apart.
LOCK_SKIP_TIER = "tier lock"
LOCK_SKIP_SQLITE_WRITER = "sqlite_writer lock"
LOCK_SKIP_UNSPECIFIED = "lock"


class _LockSkipped(Exception):  # noqa: N818
    """Internal control-flow signal: a scheduler job was skipped on a lock timeout.

    Carries *which* lock timed out. ``_scheduler_job_lock`` raises it for two
    different reasons -- the tier lock, and on SQLite deployments only the writer
    lock -- and a guard that reported the sqlite writer unconditionally sent
    operators looking at ``SQLITE_WRITE_LOCK`` during a ``MAINTENANCE_LOCK``
    contention. The production PostgreSQL deployment does not take that lock at
    all, so the old message named a lock that was never involved.

    The default keeps bare ``raise _LockSkipped`` working; callers that know the
    cause should pass one.

    The message must never contain the string ``_LockSkipped``: it is a forbidden
    signature in ``scripts/check_p1p2_lock_health.py``, where it means an
    *escaped* skip. Naming the class here would turn every handled skip into a
    false alarm.
    """

    def __init__(self, cause: str = LOCK_SKIP_UNSPECIFIED) -> None:
        super().__init__(cause)
        self.cause = cause


def _with_lock_skip_guard(func: Callable[..., object]) -> Callable[..., object]:
    """Catch ``_LockSkipped`` and log a clean warning."""

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> object:
        try:
            return func(*args, **kwargs)
        except _LockSkipped as exc:
            logger.warning("Job %s skipped: %s timed out", getattr(func, "__name__", "unknown"), exc.cause)
            return None

    return wrapper


def _db_gate(urls: tuple[str, ...] | None = None) -> bool:
    """Return whether the named databases are worth starting a job for.

    A dead database must not be discovered while a tier lock is held. During the
    2026-10-03 outage the dead-letter jobs spent ~150s each on connect retries
    under ``MAINTENANCE_LOCK``, and every other maintenance job queued behind them
    hit the 60s lock timeout and skipped -- so one unreachable database turned
    into a fully missed maintenance window, including the jobs that were not
    waiting on the database at all.

    The probe therefore runs *before* the lock, and returning ``False`` is a
    quiet skip: it raises nothing, so tenacity does not retry and no failure
    alert fires. Alerting belongs to the Prometheus ``kbo_db_available`` gauge,
    because the incident ledger this repo would otherwise write to is itself in
    the database that is down.
    """
    return _DB_GATE.check(urls if urls is not None else _operational_urls())


def _reset_db_gate() -> None:
    """Forget every memoised probe result. For tests that assert both outcomes."""
    _DB_GATE.reset()


@overload
def _with_db_fail_fast_guard[**P, R](
    func: Callable[P, R],
    *,
    urls: Callable[[], tuple[str, ...]] | None = None,
) -> Callable[P, R]: ...


@overload
def _with_db_fail_fast_guard[**P, R](
    func: None = None,
    *,
    urls: Callable[[], tuple[str, ...]],
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def _with_db_fail_fast_guard(
    func: Callable[..., object] | None = None,
    *,
    urls: Callable[[], tuple[str, ...]] | None = None,
) -> Callable[..., object]:
    """Skip a DB-bound job before it takes a tier lock, while the database is down.

    Placement is the whole contract: this decorator goes *outside*
    ``_with_lock_skip_guard`` and ``retry``, so the probe runs before either.
    Inside, a job would already be holding the lock it was meant to protect, and
    tenacity would turn one outage into hours of retries against a socket that is
    not going to answer.

    Usable bare (``@_with_db_fail_fast_guard``) or with arguments
    (``@_with_db_fail_fast_guard(urls=...)``). Both spellings exist because most
    jobs need only the operational database and spelling that out on twenty-three
    of them would bury the two that do not.

    Args:
        func: The job to guard, when used bare.
        urls: Supplies the databases this job needs, as an ordered tuple so the
            memo can key on it. A callable rather than a value because a
            deployment's target set is resolved from the environment at call
            time, and resolving it at import would freeze whatever the importing
            process happened to see. Defaults to the operational database.

    Returns:
        The wrapped job, or the decorator when called with arguments.

    """

    def decorate(target: Callable[..., object]) -> Callable[..., object]:
        resolve = urls if urls is not None else _operational_urls

        @functools.wraps(target)
        def wrapper(*args: object, **kwargs: object) -> object:
            if not _db_gate(resolve()):
                logger.warning("Job %s skipped: database unreachable", getattr(target, "__name__", "unknown"))
                return None
            return target(*args, **kwargs)

        return wrapper

    return decorate if func is None else decorate(func)


def _operational_urls() -> tuple[str, ...]:
    """Return the operational database URL, read at call time.

    ``Engine.url`` rather than the ``DATABASE_URL`` constant so the gate asks the
    engine it is about to use, not the string it was built from.
    """
    return (Engine.url.render_as_string(hide_password=False),)


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

    If the tier lock, or on SQLite additionally the writer lock, cannot be
    acquired within ``SQLITE_WRITE_LOCK_TIMEOUT_SECONDS``, the job raises
    ``_LockSkipped`` carrying which one it was so it can release the tier lock,
    log the skip with the real cause, and let the scheduler retry on the next
    cycle.
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
        raise _LockSkipped(LOCK_SKIP_TIER)
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
                raise _LockSkipped(LOCK_SKIP_SQLITE_WRITER)
            yield
    finally:
        tier_lock.release()


def lock_skip_monitor_job() -> None:
    """Monitor lock skip rate and open an incident when threshold is exceeded."""
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

    try:
        try:
            metrics = counter.collect()
        except (AttributeError, RuntimeError, OSError):
            logger.info("Lock skip metric not registered yet; skipping check")
            return
        if not metrics:
            return

        from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
        from src.notifications.bridge import apply_incidents

        events: list[AlertEvent] = []
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
                    logger.warning(
                        "[LockSkipAlert] High lock contention: job=%s lock=%s skips_in_interval=%d (threshold=%d)",
                        job_id,
                        lock_name,
                        int(delta),
                        threshold,
                    )
                    events.append(
                        AlertEvent(
                            source=AlertSource.SCHEDULER,
                            component=f"lock:{lock_name}",
                            severity=AlertSeverity.WARNING,
                            title=f"스케줄러 락 경합: {job_id}",
                            message=f"{int(delta)}회 스킵 (lock={lock_name}, threshold={threshold})",
                            incident_key=f"{LOCK_SKIP_KEY_PREFIX}{job_id}:{lock_name}",
                            metadata={
                                "job_id": job_id,
                                "lock": lock_name,
                                "skips_in_interval": int(delta),
                                "threshold": threshold,
                            },
                        ),
                    )

        # Reconcile only within the lock-skip namespace; a run with no
        # over-threshold keys recovers the previous ones.
        apply_incidents(events, reconcile_prefix=LOCK_SKIP_KEY_PREFIX)
        if events:
            logger.warning("[LockSkipAlert] Opened/renewed %d lock-skip incident(s)", len(events))
        else:
            logger.info("=== Lock Skip Rate Check Passed (no excessive skips) ===")
    except ALERT_EXCEPTIONS:
        logger.exception("Error during lock skip monitor check")
