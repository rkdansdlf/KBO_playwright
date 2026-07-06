from __future__ import annotations

from unittest.mock import MagicMock

from scripts import scheduler


def test_sqlite_writer_lock_acquires_for_sqlite(monkeypatch):
    mock_lock = MagicMock()
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", mock_lock)

    with scheduler._sqlite_writer_lock():
        pass

    mock_lock.__enter__.assert_called_once()
    mock_lock.__exit__.assert_called_once()


def test_sqlite_writer_lock_noop_for_postgres(monkeypatch):
    mock_lock = MagicMock()
    monkeypatch.setenv("DATABASE_URL", "postgresql://host/db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", mock_lock)

    with scheduler._sqlite_writer_lock():
        pass

    mock_lock.__enter__.assert_not_called()
    mock_lock.__exit__.assert_not_called()


def test_scheduler_job_lock_nests_tier_and_sqlite_locks(monkeypatch):
    tier_lock = MagicMock()
    sqlite_lock = MagicMock()
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", sqlite_lock)

    with scheduler._scheduler_job_lock(tier_lock):
        pass

    tier_lock.__enter__.assert_called_once()
    tier_lock.__exit__.assert_called_once()
    sqlite_lock.__enter__.assert_called_once()
    sqlite_lock.__exit__.assert_called_once()
