import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils.lock import ForceProcessLock, LockAcquisitionError, ProcessLock


@pytest.fixture(autouse=True)
def _clean_env_pg(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure standard tests do not use PG advisory locks unless explicitly mocked."""
    monkeypatch.delenv("OCI_DB_URL", raising=False)
    monkeypatch.delenv("TARGET_DATABASE_URL", raising=False)
    db_url = os.getenv("DATABASE_URL", "")
    if "postgresql" in db_url:
        monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")


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


def test_shared_singleton_blocking_acquire_across_threads(tmp_path: Path) -> None:
    """A single shared ProcessLock instance must serialize (not spuriously fail) across threads.

    Regression test for the scheduler ``sqlite_writer`` failure: when a
    module-level singleton lock was held by one APScheduler worker thread and a
    second worker thread attempted a *blocking* acquire on the same instance,
    the instance-level ``thread_lock_acquired`` flag made the second thread
    return immediately (raising ``LockAcquisitionError``) instead of blocking
    until the first thread released. Per-thread state must fix this.
    """
    shared_lock = ProcessLock("test_shared_singleton", lock_dir=tmp_path)

    order: list[str] = []
    errors: list[Exception] = []
    started = threading.Event()

    def holder() -> None:
        with shared_lock:
            order.append("holder-acquired")
            started.set()
            # Hold the lock long enough for the waiter to attempt acquisition.
            time.sleep(0.3)
            order.append("holder-releasing")

    def waiter() -> None:
        started.wait(timeout=2)
        try:
            # Blocking acquire on the SAME shared instance from another thread.
            with shared_lock:
                order.append("waiter-acquired")
        except (LockAcquisitionError, OSError, RuntimeError) as exc:
            errors.append(exc)

    t_holder = threading.Thread(target=holder)
    t_waiter = threading.Thread(target=waiter)
    t_holder.start()
    t_waiter.start()
    t_holder.join(timeout=5)
    t_waiter.join(timeout=5)

    assert errors == [], f"blocking acquire from another thread should not fail: {errors}"
    # The waiter must only acquire after the holder released.
    assert order == ["holder-acquired", "holder-releasing", "waiter-acquired"]


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


@pytest.mark.slow
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
    with subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as p:
        assert p.stdout is not None
        # Wait for the subprocess to print ACQUIRED
        line = p.stdout.readline().strip()
        assert line == "ACQUIRED"

        # Now try to acquire the lock in the main process with blocking=False
        main_lock = ProcessLock(lock_name, lock_dir=tmp_path, blocking=False)
        assert main_lock.acquire() is False

        # Wait for the subprocess to finish and close pipes
        p.communicate()

    # Now the main process should be able to acquire the lock
    assert main_lock.acquire() is True
    main_lock.release()


def test_postgresql_advisory_lock_acquire_release() -> None:
    """Test that ProcessLock calls pg_try_advisory_lock or pg_advisory_lock when PostgreSQL is detected."""
    from unittest.mock import MagicMock, patch

    lock_name = "test_pg_lock"

    mock_connection = MagicMock()
    mock_connection.execute.return_value.scalar.return_value = True

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_connection

    with (
        patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
        patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
    ):
        lock = ProcessLock(lock_name, blocking=False)
        assert lock.acquire() is True

        mock_connection.execute.assert_called()
        call_args = mock_connection.execute.call_args[0][0]
        assert "pg_try_advisory_lock" in str(call_args)

        lock.release()

        last_call_args = mock_connection.execute.call_args[0][0]
        assert "pg_advisory_unlock" in str(last_call_args)


def test_postgresql_advisory_lock_blocking() -> None:
    """Test that ProcessLock calls pg_advisory_lock when blocking=True."""
    from unittest.mock import MagicMock, patch

    lock_name = "test_pg_lock_blocking"

    mock_connection = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_connection

    with (
        patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
        patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
    ):
        lock = ProcessLock(lock_name, blocking=True)
        assert lock.acquire() is True

        mock_connection.execute.assert_called()
        call_args = mock_connection.execute.call_args[0][0]
        assert "pg_advisory_lock" in str(call_args)

        lock.release()


def test_postgresql_advisory_lock_fallback_on_failure(tmp_path: Path) -> None:
    """Test that ProcessLock falls back to fcntl file locking if database connection raises an error."""
    from unittest.mock import MagicMock, patch
    from sqlalchemy.exc import SQLAlchemyError

    lock_name = "test_pg_lock_fallback"

    mock_engine = MagicMock()
    mock_engine.connect.side_effect = SQLAlchemyError("DB Connection Down")

    with (
        patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
        patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
    ):
        lock = ProcessLock(lock_name, lock_dir=tmp_path, blocking=False)
        assert lock.acquire() is True
        assert lock.lock_file_path.exists()
        lock.release()


def test_force_process_lock_retries_after_stale_clear(tmp_path: Path) -> None:
    """When the base acquire fails, ForceProcessLock must clear a stale lock and retry once."""
    lock = ForceProcessLock("test_force_retry", lock_dir=tmp_path)

    with (
        patch.object(ForceProcessLock, "_clear_stale_lock") as clear_mock,
        patch.object(ProcessLock, "acquire", side_effect=[False, True]) as acquire_mock,
    ):
        assert lock.acquire(blocking=False) is True

    clear_mock.assert_called_once()
    assert acquire_mock.call_count == 2


def test_force_process_lock_reentry_does_not_force_clear(tmp_path: Path) -> None:
    """A same-thread re-acquire must return False without attempting a force-clear."""
    lock = ForceProcessLock("test_force_reentry", lock_dir=tmp_path)
    assert lock.acquire() is True

    with patch.object(ForceProcessLock, "_clear_stale_lock") as clear_mock:
        assert lock.acquire(blocking=False) is False

    clear_mock.assert_not_called()
    lock.release()


def test_force_process_lock_succeeds_after_stale_file_present(tmp_path: Path) -> None:
    """A stale lock file (dead PID) must not block acquisition."""
    lock = ForceProcessLock("test_force_stale", lock_dir=tmp_path)
    lock.lock_file_path.write_text("999999\n", encoding="utf-8")

    assert lock.acquire(blocking=False) is True
    lock.release()
    assert lock.acquire(blocking=False) is True
    lock.release()


def test_multiple_threads_serialize_on_shared_lock(tmp_path: Path) -> None:
    """Three threads sharing one lock instance must acquire strictly one-at-a-time."""
    shared = ProcessLock("test_multi_thread", lock_dir=tmp_path)
    order: list[int] = []
    errors: list[Exception] = []
    gate = threading.Barrier(3)

    def worker(wid: int) -> None:
        gate.wait(timeout=5)
        try:
            with shared:
                order.append(wid)
                time.sleep(0.1)
        except (LockAcquisitionError, OSError, RuntimeError) as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert errors == [], f"concurrent acquire should not fail: {errors}"
    assert sorted(order) == [0, 1, 2]
    assert len(order) == 3


def test_blocking_acquire_without_timeout_uses_lock_default(tmp_path: Path) -> None:
    """A blocking acquire without timeout must not pass ``None`` to ``Lock.acquire``."""
    lock = ProcessLock("test_blocking_default_timeout", lock_dir=tmp_path)

    assert lock.acquire() is True
    lock.release()
