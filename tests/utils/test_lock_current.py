from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

import src.utils.lock as lock_module
from src.utils.lock import ForceProcessLock, ProcessLock


@pytest.fixture(autouse=True)
def _clean_env_pg(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep lock tests on the local file-lock path despite repository dotenv config."""
    for key in ("OCI_DB_URL", "TARGET_DATABASE_URL", "DATABASE_URL"):
        monkeypatch.delenv(key, raising=False)


def test_shared_lock_state_is_isolated_between_threads(tmp_path: Path) -> None:
    """A shared lock keeps ownership and descriptors local to each thread."""
    shared_lock = ProcessLock("test_thread_local_state", lock_dir=tmp_path)
    holder_ready = threading.Event()
    waiter_attempting = threading.Event()
    release_holder = threading.Event()
    waiter_finished = threading.Event()
    states: dict[str, tuple[bool, object | None, int, object | None]] = {}
    results: list[bool] = []
    errors: list[BaseException] = []

    def holder() -> None:
        acquired = False
        try:
            acquired = shared_lock.acquire()
            assert acquired is True
            states["holder_active"] = (
                shared_lock.thread_lock_acquired,
                shared_lock.file_fd,
                shared_lock.acquire_count,
                shared_lock.db_connection,
            )
            holder_ready.set()
            assert release_holder.wait(timeout=2)
        except BaseException as exc:
            errors.append(exc)
            holder_ready.set()
        finally:
            if acquired:
                shared_lock.release()
            states["holder_released"] = (
                shared_lock.thread_lock_acquired,
                shared_lock.file_fd,
                shared_lock.acquire_count,
                shared_lock.db_connection,
            )

    def waiter() -> None:
        try:
            assert holder_ready.wait(timeout=2)
            states["waiter_before"] = (
                shared_lock.thread_lock_acquired,
                shared_lock.file_fd,
                shared_lock.acquire_count,
                shared_lock.db_connection,
            )
            waiter_attempting.set()
            results.append(shared_lock.acquire(timeout=2))
            shared_lock.release()
            states["waiter_released"] = (
                shared_lock.thread_lock_acquired,
                shared_lock.file_fd,
                shared_lock.acquire_count,
                shared_lock.db_connection,
            )
        except BaseException as exc:
            errors.append(exc)
        finally:
            waiter_finished.set()

    holder_thread = threading.Thread(target=holder)
    waiter_thread = threading.Thread(target=waiter)
    holder_thread.start()
    assert holder_ready.wait(timeout=2)
    waiter_thread.start()
    assert waiter_attempting.wait(timeout=2)
    release_holder.set()
    assert waiter_finished.wait(timeout=2)
    holder_thread.join(timeout=2)
    waiter_thread.join(timeout=2)

    assert errors == []
    assert not holder_thread.is_alive()
    assert not waiter_thread.is_alive()
    assert results == [True]
    assert states["holder_active"][0] is True
    assert states["holder_active"][2] == 1
    assert states["waiter_before"] == (False, None, 0, None)
    assert states["holder_released"] == (False, None, 0, None)
    assert states["waiter_released"] == (False, None, 0, None)
    if lock_module.HAS_FCNTL:
        assert states["holder_active"][1] is not None


def test_lock_state_properties_update_thread_local_state(tmp_path: Path) -> None:
    """The public state properties read and write the current thread state."""
    lock = ProcessLock("test_state_properties", lock_dir=tmp_path)
    connection = object()

    lock.file_fd = None
    lock.acquire_count = 3
    lock.db_connection = connection

    assert lock.file_fd is None
    assert lock.acquire_count == 3
    assert lock.db_connection is connection


def test_blocking_file_lock_timeout_releases_thread_lock(tmp_path: Path) -> None:
    """A file-lock timeout returns false and clears acquisition bookkeeping."""
    lock = ProcessLock("test_file_timeout", lock_dir=tmp_path)

    with (
        patch.object(lock, "_acquire_file_lock", return_value=False) as acquire_file,
        patch("src.utils.lock.time.monotonic", side_effect=[10.0, 10.2]),
        patch("src.utils.lock.time.sleep") as sleep,
    ):
        assert lock.acquire(timeout=0.1) is False

    acquire_file.assert_called_once_with(blocking=False)
    sleep.assert_not_called()
    assert lock.thread_lock_acquired is False
    assert lock.acquire_count == 0
    assert lock.file_fd is None


def test_blocking_file_lock_retries_without_sleeping_in_test(tmp_path: Path) -> None:
    """A transient file-lock failure retries before succeeding."""
    lock = ProcessLock("test_file_retry", lock_dir=tmp_path)

    with (
        patch.object(lock, "_acquire_file_lock", side_effect=[False, True]) as acquire_file,
        patch("src.utils.lock.time.monotonic", side_effect=[10.0, 10.01]),
        patch("src.utils.lock.time.sleep") as sleep,
    ):
        assert lock.acquire(timeout=1.0) is True
        lock.release()

    assert acquire_file.call_args_list == [call(blocking=False), call(blocking=False)]
    sleep.assert_called_once_with(0.05)


def test_file_lock_failure_cleans_up_acquire_state(tmp_path: Path) -> None:
    """An fcntl failure closes the descriptor and releases the thread lock."""
    if not lock_module.HAS_FCNTL:
        pytest.skip("fcntl is required for the file-lock failure path")

    lock = ProcessLock("test_file_flock_failure", lock_dir=tmp_path)
    with patch.object(lock_module.fcntl, "flock", side_effect=OSError("lock failed")):
        assert lock.acquire(blocking=True) is False

    lock.release()
    assert lock.file_fd is None
    assert lock.thread_lock_acquired is False
    assert lock.acquire_count == 0


def test_postgresql_advisory_lock_timeout_cleans_up_connection(tmp_path: Path) -> None:
    """A PostgreSQL advisory-lock timeout closes its connection and thread state."""
    mock_connection = MagicMock()
    mock_connection.execute.return_value.scalar.return_value = False
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_connection
    lock = ProcessLock("test_pg_timeout", lock_dir=tmp_path)

    with (
        patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
        patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        patch("src.utils.lock.time.monotonic", side_effect=[10.0, 10.2]),
        patch("src.utils.lock.time.sleep") as sleep,
    ):
        assert lock.acquire(timeout=0.1) is False

    mock_connection.close.assert_called_once()
    sleep.assert_not_called()
    assert lock.db_connection is None
    assert lock.thread_lock_acquired is False
    assert lock.acquire_count == 0
    assert lock.file_fd is None


def test_postgresql_advisory_lock_retries_without_live_database(tmp_path: Path) -> None:
    """A failed PostgreSQL try-lock is retried and can then succeed."""
    mock_connection = MagicMock()
    mock_connection.execute.return_value.scalar.side_effect = [False, True]
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_connection
    lock = ProcessLock("test_pg_retry", lock_dir=tmp_path)

    with (
        patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
        patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        patch("src.utils.lock.time.monotonic", side_effect=[10.0, 10.01]),
        patch("src.utils.lock.time.sleep") as sleep,
    ):
        assert lock.acquire(timeout=1.0) is True
        lock.release()

    assert mock_connection.execute.call_count == 3
    sleep.assert_called_once_with(0.05)
    mock_connection.close.assert_called_once()


def test_release_cleans_up_after_postgresql_unlock_failure(tmp_path: Path) -> None:
    """An advisory unlock error still closes the connection and clears state."""
    mock_connection = MagicMock()
    mock_connection.execute.side_effect = [None, OSError("unlock failed")]
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_connection
    lock = ProcessLock("test_pg_unlock_failure", lock_dir=tmp_path)

    with (
        patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
        patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
    ):
        assert lock.acquire() is True
        lock.release()

    mock_connection.close.assert_called_once()
    assert lock.db_connection is None
    assert lock.thread_lock_acquired is False
    assert lock.acquire_count == 0


def test_release_cleans_up_after_sqlalchemy_unlock_failure(tmp_path: Path) -> None:
    """A SQLAlchemy unlock failure must not mask the original job failure."""
    mock_connection = MagicMock()
    mock_connection.execute.side_effect = [None, SQLAlchemyError("connection closed")]
    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_connection
    lock = ProcessLock("test_pg_sqlalchemy_unlock_failure", lock_dir=tmp_path)

    with (
        patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
        patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
    ):
        assert lock.acquire() is True
        lock.release()

    mock_connection.close.assert_called_once()
    assert lock.db_connection is None
    assert lock.thread_lock_acquired is False


def test_force_lock_retries_with_timeout_argument(tmp_path: Path) -> None:
    """ForceProcessLock forwards timeout on both normal and retry attempts."""
    lock = ForceProcessLock("test_force_timeout_retry", lock_dir=tmp_path)
    base_acquire = MagicMock(side_effect=[False, True])

    with (
        patch.object(ProcessLock, "acquire", base_acquire),
        patch.object(lock, "_clear_stale_lock") as clear_stale,
    ):
        assert lock.acquire(blocking=False, timeout=0.25) is True

    assert base_acquire.call_args_list == [
        call(blocking=False, timeout=0.25),
        call(blocking=False, timeout=0.25),
    ]
    clear_stale.assert_called_once()


def test_force_lock_does_not_displace_active_owner_in_another_thread(tmp_path: Path) -> None:
    """A non-blocking ForceProcessLock attempt cannot displace an active owner."""
    lock = ForceProcessLock("test_force_active_owner", lock_dir=tmp_path, blocking=False)
    holder_ready = threading.Event()
    release_holder = threading.Event()
    waiter_finished = threading.Event()
    results: list[bool] = []
    errors: list[BaseException] = []

    def holder() -> None:
        acquired = False
        try:
            acquired = lock.acquire(blocking=False)
            assert acquired is True
            holder_ready.set()
            assert release_holder.wait(timeout=2)
        except BaseException as exc:
            errors.append(exc)
            holder_ready.set()
        finally:
            if acquired:
                lock.release()

    def waiter() -> None:
        try:
            assert holder_ready.wait(timeout=2)
            results.append(lock.acquire(blocking=False))
        except BaseException as exc:
            errors.append(exc)
        finally:
            waiter_finished.set()

    holder_thread = threading.Thread(target=holder)
    waiter_thread = threading.Thread(target=waiter)
    holder_thread.start()
    assert holder_ready.wait(timeout=2)
    waiter_thread.start()
    assert waiter_finished.wait(timeout=2)
    assert results == [False]
    assert lock.lock_file_path.exists()
    release_holder.set()
    holder_thread.join(timeout=2)
    waiter_thread.join(timeout=2)

    assert errors == []
    assert not holder_thread.is_alive()
    assert not waiter_thread.is_alive()
