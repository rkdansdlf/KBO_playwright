"""유틸리티: lock."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Self

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


class ProcessLock:
    """A cross-process and cross-thread lock.

    Uses threading.Lock to serialize threads within the same process,
    and Unix fcntl.flock to serialize across different processes.
    """

    def __init__(
        self,
        name: str,
        lock_dir: Path | str | None = None,
        *,
        blocking: bool = True,
    ) -> None:
        """Initializes a new instance."""
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

        self.file_fd = None
        self.thread_lock_acquired = False
        self.acquire_count = 0

    def acquire(self, *, blocking: bool | None = None) -> bool:
        """Acquire the lock.

        Args:
            blocking: Override the instance default for this acquire call.

        Returns:
            True if the lock was successfully acquired, False otherwise.

        """
        if self.thread_lock_acquired:
            logger.debug("ProcessLock is already held by this instance: %s", self.name)
            return False

        effective_blocking = self.blocking if blocking is None else blocking

        # 1. Acquire thread-level lock
        acquired = self.thread_lock.acquire(blocking=effective_blocking)
        if not acquired:
            logger.debug("Failed to acquire thread lock for: %s", self.name)
            return False

        self.thread_lock_acquired = True
        self.acquire_count = 1

        # 2. Acquire process-level file lock
        if not HAS_FCNTL:
            # Fallback when fcntl is not available (e.g. Windows)
            return True

        try:
            self.lock_dir.mkdir(parents=True, exist_ok=True)
            self.file_fd = self.lock_file_path.open("w")

            flags = fcntl.LOCK_EX
            if not effective_blocking:
                flags |= fcntl.LOCK_NB

            fcntl.flock(self.file_fd, flags)
        except OSError as e:
            logger.debug("Failed to acquire ProcessLock: %s. Error: %s", self.name, e)
            if self.file_fd:
                self.file_fd.close()
                self.file_fd = None
            self.thread_lock.release()
            self.thread_lock_acquired = False
            self.acquire_count = 0
            return False
        else:
            logger.debug("Successfully acquired ProcessLock: %s", self.name)
            return True

    def release(self) -> None:
        """Release the lock."""
        if not self.thread_lock_acquired:
            return

        # Release process-level file lock
        if HAS_FCNTL and self.file_fd:
            try:
                fcntl.flock(self.file_fd, fcntl.LOCK_UN)
                self.file_fd.close()
            except (OSError, ValueError) as e:
                logger.warning("Error releasing process lock for %s: %s", self.name, e)
            finally:
                self.file_fd = None

        # Release thread-level lock
        if self.thread_lock_acquired:
            try:
                self.thread_lock.release()
            except RuntimeError:
                # Handle case where lock is already released or owned by another thread
                pass
            finally:
                self.thread_lock_acquired = False
                self.acquire_count = 0
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
        """Exits the runtime context."""
        self.release()
