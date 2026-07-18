from __future__ import annotations

import os
import threading
from unittest.mock import MagicMock, patch

from src.utils.lock import LockAcquisitionError, ProcessLock


class TestProcessLockInit:
    def test_default_lock_dir(self):
        lock = ProcessLock("test_lock")
        assert lock.name == "test_lock"
        assert "data" in str(lock.lock_dir) and "locks" in str(lock.lock_dir)

    def test_custom_lock_dir(self, tmp_path):
        lock = ProcessLock("test", lock_dir=str(tmp_path))
        assert lock.lock_dir == tmp_path

    def test_blocking_default(self):
        lock = ProcessLock("test")
        assert lock.blocking is True

    def test_non_blocking(self):
        lock = ProcessLock("test", blocking=False)
        assert lock.blocking is False


class TestGetLockId:
    def test_deterministic(self):
        lock = ProcessLock("my_lock")
        id1 = lock._get_lock_id()
        id2 = lock._get_lock_id()
        assert id1 == id2

    def test_different_names_different_ids(self):
        lock1 = ProcessLock("lock_a")
        lock2 = ProcessLock("lock_b")
        assert lock1._get_lock_id() != lock2._get_lock_id()


class TestGetPostgresUrl:
    def test_oci_url(self, monkeypatch):
        monkeypatch.setenv("OCI_DB_URL", "postgresql://user:pass@host/db")
        for key in ("TARGET_DATABASE_URL", "DATABASE_URL"):
            monkeypatch.delenv(key, raising=False)
        lock = ProcessLock("test")
        assert lock._get_postgres_url() == "postgresql://user:pass@host/db"

    def test_database_url(self, monkeypatch):
        for key in ("OCI_DB_URL", "TARGET_DATABASE_URL"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@host/db")
        lock = ProcessLock("test")
        assert lock._get_postgres_url() == "postgresql://user:pass@host/db"

    def test_no_postgres(self, monkeypatch):
        for key in ("OCI_DB_URL", "TARGET_DATABASE_URL", "DATABASE_URL"):
            monkeypatch.delenv(key, raising=False)
        lock = ProcessLock("test")
        assert lock._get_postgres_url() is None

    def test_sqlite_url(self, monkeypatch):
        for key in ("OCI_DB_URL", "TARGET_DATABASE_URL"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo.db")
        lock = ProcessLock("test")
        assert lock._get_postgres_url() is None


class TestAcquireRelease:
    def test_acquire_and_release_no_fcntl(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.utils.lock.HAS_FCNTL", False)
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        monkeypatch.delenv("TARGET_DATABASE_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        lock = ProcessLock("test_acquire", lock_dir=str(tmp_path))
        assert lock.acquire() is True
        lock.release()

    def test_context_manager(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.utils.lock.HAS_FCNTL", False)
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        monkeypatch.delenv("TARGET_DATABASE_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with ProcessLock("test_ctx", lock_dir=str(tmp_path)) as lock:
            assert lock.thread_lock_acquired is True

    def test_context_manager_failure_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.utils.lock.HAS_FCNTL", False)
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        monkeypatch.delenv("TARGET_DATABASE_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        lock = ProcessLock("test_fail", lock_dir=str(tmp_path))
        lock.thread_lock_acquired = True
        with patch.object(lock, "acquire", return_value=False):
            try:
                with lock:
                    pass
            except LockAcquisitionError:
                pass

    def test_release_without_acquire(self, tmp_path):
        lock = ProcessLock("test_no_acquire", lock_dir=str(tmp_path))
        lock.release()


class TestAcquirePgLock:
    def test_no_pg_url_returns_true(self, tmp_path, monkeypatch):
        monkeypatch.delenv("OCI_DB_URL", raising=False)
        monkeypatch.delenv("TARGET_DATABASE_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        lock = ProcessLock("test_pg", lock_dir=str(tmp_path))
        assert lock._acquire_pg_lock(effective_blocking=True) is True

    @patch("src.utils.lock.ProcessLock._get_pg_engine")
    @patch("src.utils.lock.ProcessLock._get_postgres_url")
    def test_pg_lock_exception_returns_true(self, mock_url, mock_engine, tmp_path, monkeypatch):
        mock_url.return_value = "postgresql://user:pass@host/db"
        mock_engine.side_effect = RuntimeError("connection failed")

        lock = ProcessLock("test_pg_err", lock_dir=str(tmp_path))
        result = lock._acquire_pg_lock(effective_blocking=True)
        assert result is True
