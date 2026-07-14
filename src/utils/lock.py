"""Lock utilities for cross-process synchronization."""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import struct
import threading
from pathlib import Path
from typing import IO, TYPE_CHECKING, ClassVar, Self, cast

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(__name__)

# Try importing fcntl for Unix/macOS file locking
try:
    import fcntl

    HAS_FCNTL = True
except ImportError:
    HAS_FCNTL = False
    logger.warning("fcntl module not available. ProcessLock will fall back to thread-only locks.")

_thread_locks: dict[str, threading.Lock] = {}
_thread_locks_lock = threading.Lock()


class LockAcquisitionError(Exception):
    """Raised when a lock cannot be acquired."""


class _LockState(threading.local):
    """Per-thread acquisition state for a ProcessLock instance.

    A single ProcessLock instance is frequently shared as a module-level
    singleton across APScheduler's thread pool. The underlying threading.Lock
    (stored in ``_thread_locks``) already provides correct cross-thread mutual
    exclusion, but the acquisition bookkeeping (whether *this* thread currently
    holds the lock, its file descriptor and DB connection) must be tracked
    per-thread. Otherwise one thread acquiring the lock would make every other
    thread believe the lock is already held by itself and return early instead
    of blocking, raising a spurious LockAcquisitionError.
    """

    def __init__(self) -> None:
        self.file_fd: IO[str] | None = None
        self.thread_lock_acquired = False
        self.acquire_count = 0
        self.db_connection = None


class ProcessLock:
    """A cross-process and cross-thread lock supporting local fcntl and PG advisory locks."""

    _pg_engines: ClassVar[dict[str, object]] = {}
    _pg_engines_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        name: str,
        lock_dir: Path | str | None = None,
        *,
        blocking: bool = True,
    ) -> None:
        """Initialize a new ProcessLock instance."""
        self.name = name
        self.blocking = blocking

        if lock_dir is None:
            # Default to the data/locks directory in the workspace root
            self.lock_dir = Path(__file__).resolve().parents[2] / "data" / "locks"
        else:
            self.lock_dir = Path(lock_dir)

        self.lock_file_path = self.lock_dir / f"{name}.lock"

        # Thread lock serialization (process-wide)
        with _thread_locks_lock:
            if name not in _thread_locks:
                _thread_locks[name] = threading.Lock()
            self.thread_lock = _thread_locks[name]

        # Per-thread acquisition state (see _LockState docstring).
        self._state = _LockState()

    @property
    def file_fd(self) -> IO[str] | None:
        """Return the current thread's lock file descriptor."""
        return self._state.file_fd

    @file_fd.setter
    def file_fd(self, value: IO[str] | None) -> None:
        self._state.file_fd = value

    @property
    def thread_lock_acquired(self) -> bool:
        """Return whether the current thread owns the lock."""
        return self._state.thread_lock_acquired

    @thread_lock_acquired.setter
    def thread_lock_acquired(self, value: bool) -> None:
        self._state.thread_lock_acquired = value

    @property
    def acquire_count(self) -> int:
        """Return the current thread's nested acquisition count."""
        return self._state.acquire_count

    @acquire_count.setter
    def acquire_count(self, value: int) -> None:
        self._state.acquire_count = value

    @property
    def db_connection(self) -> object | None:
        """Return the current thread's advisory-lock connection."""
        return self._state.db_connection

    @db_connection.setter
    def db_connection(self, value: object | None) -> None:
        self._state.db_connection = value

    def _get_lock_id(self) -> int:
        """Hash the lock name to a 64-bit signed integer for pg_advisory_lock."""
        h = hashlib.sha256(self.name.encode("utf-8")).digest()
        return cast("int", struct.unpack("q", h[:8])[0])

    def _get_postgres_url(self) -> str | None:
        """Dynamically detect PostgreSQL database URL from the environment."""
        oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL") or ""
        db_url = os.getenv("DATABASE_URL") or ""

        if "postgresql" in oci_url:
            return oci_url
        if "postgresql" in db_url:
            return db_url
        return None

    @classmethod
    def _get_pg_engine(cls, url: str) -> object:
        """Get or create a cached SQLAlchemy engine for PostgreSQL advisory locks."""
        with cls._pg_engines_lock:
            if url not in cls._pg_engines:
                from sqlalchemy import create_engine

                cls._pg_engines[url] = create_engine(url, pool_pre_ping=True)
            return cls._pg_engines[url]

    def _acquire_pg_lock(self, *, effective_blocking: bool) -> bool:
        """Acquire PostgreSQL advisory lock if configured.

        Returns True if lock acquired or not needed.
        Returns False only if lock is held by another process and should NOT fall back.
        """
        pg_url = self._get_postgres_url()
        if not pg_url:
            return True

        from sqlalchemy import text
        from sqlalchemy.engine import Connection
        from sqlalchemy.exc import SQLAlchemyError

        success = False
        try:
            engine = self._get_pg_engine(pg_url)

            conn = engine.connect() if not isinstance(engine, Connection) else engine  # type: ignore[attr-defined]

            self._state.db_connection = conn  # type: ignore[assignment]
            lock_id = self._get_lock_id()

            if effective_blocking:
                conn.execute(text("SELECT pg_advisory_lock(:key)"), {"key": lock_id})
                success = True
            else:
                res = conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_id}).scalar()
                if res:
                    logger.debug("Successfully acquired PostgreSQL advisory lock for: %s", self.name)
                    success = True
                else:
                    logger.warning(
                        "PostgreSQL advisory lock held by another process for %s, falling back to file lock",
                        self.name,
                    )
                    conn.close()
                    self._state.db_connection = None
                    return True
        except (SQLAlchemyError, OSError, RuntimeError) as e:
            logger.warning(
                "Failed to acquire PostgreSQL advisory lock for %s, falling back to local lock: %s",
                self.name,
                e,
            )
            if self._state.db_connection is not None:
                with contextlib.suppress(Exception):
                    self._state.db_connection.close()
                self._state.db_connection = None
            success = True

        return success

    def acquire(self, *, blocking: bool | None = None) -> bool:
        """Acquire the lock."""
        if self._state.thread_lock_acquired:
            logger.debug("ProcessLock is already held by this instance: %s", self.name)
            return False

        effective_blocking = self.blocking if blocking is None else blocking

        # 1. Acquire thread-level lock
        acquired = self.thread_lock.acquire(blocking=effective_blocking)
        if not acquired:
            logger.debug("Failed to acquire thread lock for: %s", self.name)
            return False

        self._state.thread_lock_acquired = True
        self._state.acquire_count = 1

        # 2. Try acquiring PostgreSQL advisory lock
        if not self._acquire_pg_lock(effective_blocking=effective_blocking):
            self.thread_lock.release()
            self._state.thread_lock_acquired = False
            self._state.acquire_count = 0
            logger.debug("Failed to acquire PostgreSQL advisory lock (held by other process) for: %s", self.name)
            return False

        if self._state.db_connection is not None:
            return True

        # 3. Fallback to process-level file lock
        if not HAS_FCNTL:
            # Fallback when fcntl is not available (e.g. Windows)
            return True

        success = False
        try:
            self.lock_dir.mkdir(parents=True, exist_ok=True)
            self._state.file_fd = self.lock_file_path.open("w")

            flags = fcntl.LOCK_EX
            if not effective_blocking:
                flags |= fcntl.LOCK_NB

            fcntl.flock(self._state.file_fd, flags)
            success = True
            self._state.file_fd.write(f"{os.getpid()}\n")
            self._state.file_fd.flush()
            logger.debug("Successfully acquired ProcessLock: %s", self.name)
        except OSError as e:
            logger.debug("Failed to acquire ProcessLock: %s. Error: %s", self.name, e)
            if self._state.file_fd:
                self._state.file_fd.close()
                self._state.file_fd = None
            self.thread_lock.release()
            self._state.thread_lock_acquired = False
            self._state.acquire_count = 0

        return success

    def release(self) -> None:
        """Release the lock."""
        if not self._state.thread_lock_acquired:
            return

        # 1. Release PostgreSQL advisory lock
        if self._state.db_connection is not None:
            try:
                from sqlalchemy import text

                lock_id = self._get_lock_id()
                self._state.db_connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_id})
                logger.debug("Released PostgreSQL advisory lock for: %s", self.name)
            except (OSError, RuntimeError) as e:
                logger.warning("Error releasing PostgreSQL advisory lock for %s: %s", self.name, e)
            finally:
                with contextlib.suppress(Exception):
                    self._state.db_connection.close()
                self._state.db_connection = None

        # 2. Release process-level file lock
        if HAS_FCNTL and self._state.file_fd:
            try:
                fcntl.flock(self._state.file_fd, fcntl.LOCK_UN)
            except (OSError, ValueError) as e:
                logger.warning("Error releasing process lock for %s: %s", self.name, e)
            finally:
                with contextlib.suppress(OSError, ValueError):
                    self._state.file_fd.close()
                self._state.file_fd = None

        # 3. Release thread-level lock
        if self._state.thread_lock_acquired:
            try:
                self.thread_lock.release()
            except RuntimeError:
                # Handle case where lock is already released or owned by another thread
                pass
            finally:
                self._state.thread_lock_acquired = False
                self._state.acquire_count = 0
            logger.debug("Released ProcessLock: %s", self.name)

    def __enter__(self) -> Self:
        """Enters the runtime context."""
        if not self.acquire():
            msg = f"Could not acquire ProcessLock: {self.name}"
            raise LockAcquisitionError(msg)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the runtime context."""
        self.release()


class ForceProcessLock(ProcessLock):
    """ProcessLock variant that force-acquires by clearing stale locks.

    Use this when you need to guarantee lock acquisition even if a previous
    process crashed without releasing the lock.
    """

    def acquire(self, *, blocking: bool | None = None) -> bool:
        """Acquire the lock, clearing stale file locks if needed."""
        try:
            return super().acquire(blocking=blocking)
        except LockAcquisitionError:
            logger.warning("Normal lock acquisition failed for %s, attempting force-acquire", self.name)

        logger.warning("Force-acquiring ProcessLock: %s (clearing stale lock)", self.name)
        self._clear_stale_lock()
        return super().acquire(blocking=blocking)

    def _clear_stale_lock(self) -> None:
        """Remove stale lock file if the owning process is no longer running."""
        try:
            if not self.lock_file_path.exists():
                return

            pid_str = self.lock_file_path.read_text().strip().split("\n")[0]
            if not pid_str.isdigit():
                self.lock_file_path.unlink(missing_ok=True)
                return

            pid = int(pid_str)
            try:
                os.kill(pid, 0)
            except (OSError, ProcessLookupError):
                logger.info(
                    "Removing stale lock file for %s (PID %d not running)",
                    self.name,
                    pid,
                )
                self.lock_file_path.unlink(missing_ok=True)
        except OSError as e:
            logger.debug("Error clearing stale lock for %s: %s", self.name, e)
