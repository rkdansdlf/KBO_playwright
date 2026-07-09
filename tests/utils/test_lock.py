import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from src.utils.lock import LockAcquisitionError, ProcessLock


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
