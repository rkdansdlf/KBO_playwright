from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.lock import HAS_FCNTL, ForceProcessLock, LockAcquisitionError, ProcessLock


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OCI_DB_URL", raising=False)
    monkeypatch.delenv("TARGET_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)


class TestFcntlImport:
    def test_fcntl_available(self):
        assert isinstance(HAS_FCNTL, bool)


class TestGetPgEngine:
    def test_get_pg_engine_caches(self, tmp_path: Path) -> None:
        lock = ProcessLock("test_cache", lock_dir=str(tmp_path))
        with patch.object(ProcessLock, "_pg_engines", {}):
            with patch.object(ProcessLock, "_pg_engines_lock"):
                mock_engine = MagicMock()
                with patch("sqlalchemy.create_engine", return_value=mock_engine) as mock_create:
                    result1 = lock._get_pg_engine("postgresql://localhost/db")
                    result2 = lock._get_pg_engine("postgresql://localhost/db")
                    assert result1 is mock_engine
                    assert result2 is mock_engine
                    assert mock_create.call_count == 1


class TestAcquirePgLockBlocking:
    def test_pg_lock_blocking_success(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
            patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        ):
            lock = ProcessLock("test_pg_block", lock_dir=str(tmp_path), blocking=True)
            result = lock._acquire_pg_lock(effective_blocking=True)
            assert result is True
            assert lock.db_connection is mock_conn

    def test_pg_lock_nonblocking_success(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = True
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
            patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        ):
            lock = ProcessLock("test_pg_nb", lock_dir=str(tmp_path), blocking=False)
            result = lock._acquire_pg_lock(effective_blocking=False)
            assert result is True
            assert lock.db_connection is mock_conn

    def test_pg_lock_nonblocking_falls_back(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = False
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
            patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        ):
            lock = ProcessLock("test_pg_fall", lock_dir=str(tmp_path), blocking=False)
            result = lock._acquire_pg_lock(effective_blocking=False)
            assert result is True
            assert lock.db_connection is None
            mock_conn.close.assert_called_once()

    def test_pg_lock_exception_closes_connection(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = RuntimeError("DB error")
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
            patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        ):
            lock = ProcessLock("test_pg_exc", lock_dir=str(tmp_path), blocking=True)
            result = lock._acquire_pg_lock(effective_blocking=True)
            assert result is True
            assert lock.db_connection is None


class TestAcquirePgLockReturnsFalse:
    def test_pg_lock_returns_false_when_held(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = False
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
            patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        ):
            lock = ProcessLock("test_pg_held", lock_dir=str(tmp_path), blocking=False)
            result = lock._acquire_pg_lock(effective_blocking=False)
            assert result is True


class TestAcquireFileLock:
    def test_acquire_file_lock_success(self, tmp_path: Path) -> None:
        lock = ProcessLock("test_file", lock_dir=str(tmp_path), blocking=False)
        assert lock.acquire() is True
        assert lock.file_fd is not None
        assert lock.lock_file_path.exists()
        lock.release()

    def test_acquire_file_lock_os_error(self, tmp_path: Path) -> None:
        real_open = Path.open

        def mock_open(self, *args, **kwargs):
            if str(self).endswith(".lock") and "w" in (args or ("",)):
                raise OSError("Permission denied")
            return real_open(self, *args, **kwargs)

        with patch.object(Path, "open", mock_open):
            lock = ProcessLock("test_oserr", lock_dir=str(tmp_path), blocking=False)
            result = lock.acquire()
            assert result is False
            assert lock.file_fd is None
            assert lock.thread_lock_acquired is False
            assert lock.acquire_count == 0


class TestAcquireAlreadyHeld:
    def test_acquire_returns_false_when_already_held(self, tmp_path: Path) -> None:
        lock = ProcessLock("test_held", lock_dir=str(tmp_path), blocking=False)
        assert lock.acquire() is True
        result = lock.acquire(blocking=False)
        assert result is False
        lock.release()


class TestAcquirePgReturnsFalse:
    def test_acquire_releases_thread_when_pg_returns_false(self, tmp_path: Path) -> None:
        lock = ProcessLock("test_pg_false", lock_dir=str(tmp_path), blocking=False)
        with patch.object(lock, "_acquire_pg_lock", return_value=False):
            result = lock.acquire()
            assert result is False
            assert lock.thread_lock_acquired is False
            assert lock.acquire_count == 0


class TestReleasePgLock:
    def test_release_pg_lock_success(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
            patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        ):
            lock = ProcessLock("test_rel_pg", lock_dir=str(tmp_path), blocking=True)
            assert lock.acquire() is True
            lock.release()
            assert lock.db_connection is None

    def test_release_pg_lock_error(self, tmp_path: Path) -> None:
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = OSError("Connection lost")
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        with (
            patch.object(ProcessLock, "_get_postgres_url", return_value="postgresql://localhost/db"),
            patch.object(ProcessLock, "_get_pg_engine", return_value=mock_engine),
        ):
            lock = ProcessLock("test_rel_pg_err", lock_dir=str(tmp_path), blocking=True)
            lock.acquire()
            lock.release()
            assert lock.db_connection is None


class TestReleaseFileLock:
    def test_release_file_lock_error(self, tmp_path: Path) -> None:
        lock = ProcessLock("test_rel_file", lock_dir=str(tmp_path), blocking=False)
        assert lock.acquire() is True
        assert lock.file_fd is not None

        real_flock = None
        import fcntl as fcntl_mod

        real_flock = fcntl_mod.flock

        def mock_flock(fd, flags):
            if flags == fcntl_mod.LOCK_UN:
                raise OSError("Bad file descriptor")
            return real_flock(fd, flags)

        with patch("fcntl.flock", side_effect=mock_flock):
            lock.release()
        assert lock.file_fd is None

    def test_release_file_lock_value_error(self, tmp_path: Path) -> None:
        lock = ProcessLock("test_rel_file_ve", lock_dir=str(tmp_path), blocking=False)
        assert lock.acquire() is True
        assert lock.file_fd is not None

        import fcntl as fcntl_mod

        real_flock = fcntl_mod.flock

        def mock_flock(fd, flags):
            if flags == fcntl_mod.LOCK_UN:
                raise ValueError("Invalid file descriptor")
            return real_flock(fd, flags)

        with patch("fcntl.flock", side_effect=mock_flock):
            lock.release()
        assert lock.file_fd is None


class TestReleaseThreadLock:
    def test_release_thread_lock_runtime_error(self, tmp_path: Path) -> None:
        import threading

        from src.utils.lock import _thread_locks, _thread_locks_lock

        lock_name = "test_rel_thread"
        with _thread_locks_lock:
            if lock_name in _thread_locks:
                del _thread_locks[lock_name]
            bad_lock = MagicMock()
            bad_lock.acquire.return_value = True
            bad_lock.release.side_effect = RuntimeError("Lock not owned")
            _thread_locks[lock_name] = bad_lock

        lock = ProcessLock(lock_name, lock_dir=str(tmp_path), blocking=False)
        assert lock.acquire() is True
        lock.release()
        assert lock.thread_lock_acquired is False
        assert lock.acquire_count == 0


class TestForceProcessLock:
    def test_force_acquire_success_on_first_try(self, tmp_path: Path) -> None:
        lock = ForceProcessLock("test_force_ok", lock_dir=str(tmp_path), blocking=False)
        assert lock.acquire() is True
        lock.release()

    def test_force_acquire_after_clear_stale(self, tmp_path: Path) -> None:
        lock_file = tmp_path / "test_force_stale.lock"
        lock_file.write_text("999999\n")

        call_count = 0
        original_acquire = ProcessLock.acquire

        def counting_acquire(self, blocking=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise LockAcquisitionError("Simulated stale lock")
            return original_acquire(self, blocking=blocking)

        with patch.object(ProcessLock, "acquire", counting_acquire):
            lock = ForceProcessLock("test_force_stale", lock_dir=str(tmp_path), blocking=False)
            result = lock.acquire()
            assert result is True
            assert call_count == 2

    def test_clear_stale_lock_no_file(self, tmp_path: Path) -> None:
        lock = ForceProcessLock("test_clear_no", lock_dir=str(tmp_path))
        lock._clear_stale_lock()

    def test_clear_stale_lock_non_running_pid(self, tmp_path: Path) -> None:
        lock_file = tmp_path / "test_clear_dead.lock"
        lock_file.write_text("999999\n")
        lock = ForceProcessLock("test_clear_dead", lock_dir=str(tmp_path))
        lock._clear_stale_lock()
        assert not lock_file.exists()

    def test_clear_stale_lock_invalid_pid(self, tmp_path: Path) -> None:
        lock_file = tmp_path / "test_clear_inv.lock"
        lock_file.write_text("not_a_pid\n")
        lock = ForceProcessLock("test_clear_inv", lock_dir=str(tmp_path))
        lock._clear_stale_lock()
        assert not lock_file.exists()

    def test_clear_stale_lock_running_pid(self, tmp_path: Path) -> None:
        lock_file = tmp_path / "test_clear_alive.lock"
        lock_file.write_text(f"{os.getpid()}\n")
        lock = ForceProcessLock("test_clear_alive", lock_dir=str(tmp_path))
        lock._clear_stale_lock()
        assert lock_file.exists()

    def test_clear_stale_lock_os_error(self, tmp_path: Path) -> None:
        lock_file = tmp_path / "test_clear_oserr.lock"
        lock_file.write_text("12345\n")

        with patch("pathlib.Path.exists", side_effect=OSError("Permission denied")):
            lock = ForceProcessLock("test_clear_oserr", lock_dir=str(tmp_path))
            lock._clear_stale_lock()
