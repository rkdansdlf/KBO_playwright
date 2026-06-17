from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path

from src.utils.lock import LockAcquisitionError, ProcessLock


def test_lock_basic_acquire_release(tmp_path: Path) -> None:
    """Test that we can acquire and release a ProcessLock."""
    lock_name = "test_basic"
    lock = ProcessLock(lock_name, lock_dir=tmp_path)

    assert lock.acquire() is True
    assert lock.acquire(blocking=False) is False

    lock.release()


def test_lock_context_manager(tmp_path: Path) -> None:
    """Test using ProcessLock as a context manager."""
    lock_name = "test_context"
    with ProcessLock(lock_name, lock_dir=tmp_path) as lock:
        assert lock.lock_file_path.exists()

    # After block exits, the lock should be released
    # We should be able to acquire it again in blocking=False mode
    lock2 = ProcessLock(lock_name, lock_dir=tmp_path, blocking=False)
    assert lock2.acquire() is True
    lock2.release()


def test_lock_non_blocking_fails_when_locked(tmp_path: Path) -> None:
    """Test that non-blocking acquire fails if lock is already held in another thread/process."""
    lock_name = "test_non_blocking"
    lock1 = ProcessLock(lock_name, lock_dir=tmp_path)
    assert lock1.acquire() is True

    # Try to acquire on another thread (to trigger thread lock separation)
    thread_acquired = []

    def thread_worker():
        lock2 = ProcessLock(lock_name, lock_dir=tmp_path, blocking=False)
        thread_acquired.append(lock2.acquire())
        lock2.release()

    t = threading.Thread(target=thread_worker)
    t.start()
    t.join()

    assert thread_acquired == [False]
    lock1.release()


def test_lock_context_manager_raises_when_locked(tmp_path: Path) -> None:
    """Test that context manager raises LockAcquisitionError if already locked."""
    lock_name = "test_raises"
    lock1 = ProcessLock(lock_name, lock_dir=tmp_path)
    assert lock1.acquire() is True

    # Try context manager in a thread
    thread_raised = []

    def thread_worker():
        try:
            with ProcessLock(lock_name, lock_dir=tmp_path, blocking=False):
                pass
            thread_raised.append(False)
        except LockAcquisitionError:
            thread_raised.append(True)

    t = threading.Thread(target=thread_worker)
    t.start()
    t.join()

    assert thread_raised == [True]
    lock1.release()


def test_lock_cross_process(tmp_path: Path) -> None:
    """Test that ProcessLock works across separate processes using subprocess."""
    lock_name = "test_cross_process"

    # Code to run in subprocess: acquires lock, signals parent, sleeps, then releases
    code = f"""import time, sys
from src.utils.lock import ProcessLock
lock = ProcessLock('{lock_name}', lock_dir='{tmp_path}')
if lock.acquire():
    print('ACQUIRED')
    sys.stdout.flush()
    time.sleep(2)
    lock.release()
else:
    print('FAILED')
    sys.stdout.flush()
"""

    # Start subprocess
    p = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Wait for the subprocess to print ACQUIRED
    line = p.stdout.readline().strip()
    assert line == "ACQUIRED"

    # Now try to acquire the lock in the main process with blocking=False
    main_lock = ProcessLock(lock_name, lock_dir=tmp_path, blocking=False)
    assert main_lock.acquire() is False

    # Wait for the subprocess to finish
    p.wait()

    # Now the main process should be able to acquire the lock
    assert main_lock.acquire() is True
    main_lock.release()
