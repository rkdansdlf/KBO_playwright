from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from scripts import scheduler


def test_sqlite_writer_lock_acquires_for_sqlite(monkeypatch):
    mock_lock = MagicMock()
    mock_lock.acquire.return_value = True
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", mock_lock)

    with scheduler._sqlite_writer_lock() as acquired:
        assert acquired is True

    mock_lock.acquire.assert_called_once_with(blocking=True, timeout=None)
    mock_lock.release.assert_called_once()


def test_sqlite_writer_lock_noop_for_postgres(monkeypatch):
    mock_lock = MagicMock()
    monkeypatch.setenv("DATABASE_URL", "postgresql://host/db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", mock_lock)

    with scheduler._sqlite_writer_lock() as acquired:
        assert acquired is True

    mock_lock.acquire.assert_not_called()
    mock_lock.release.assert_not_called()


def test_sqlite_writer_lock_nonblocking_skips_when_held(monkeypatch):
    mock_lock = MagicMock()
    mock_lock.acquire.return_value = False
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", mock_lock)

    with scheduler._sqlite_writer_lock(blocking=False) as acquired:
        assert acquired is False

    mock_lock.acquire.assert_called_once_with(blocking=False, timeout=None)
    mock_lock.release.assert_not_called()


def test_scheduler_job_lock_nests_tier_and_sqlite_locks(monkeypatch):
    tier_lock = MagicMock()
    tier_lock.acquire.return_value = True
    sqlite_lock = MagicMock()
    sqlite_lock.acquire.return_value = True
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", sqlite_lock)

    with scheduler._scheduler_job_lock(tier_lock):
        pass

    tier_lock.acquire.assert_called_once_with(blocking=True, timeout=scheduler.SQLITE_WRITE_LOCK_TIMEOUT_SECONDS)
    tier_lock.release.assert_called_once()
    sqlite_lock.acquire.assert_called_once_with(blocking=True, timeout=scheduler.SQLITE_WRITE_LOCK_TIMEOUT_SECONDS)
    sqlite_lock.release.assert_called_once()


def test_scheduler_job_lock_skips_when_tier_lock_times_out(monkeypatch):
    tier_lock = MagicMock()
    tier_lock.acquire.return_value = False
    sqlite_lock = MagicMock()
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", sqlite_lock)

    with pytest.raises(scheduler._LockSkipped):
        with scheduler._scheduler_job_lock(tier_lock):
            pass

    tier_lock.acquire.assert_called_once()
    tier_lock.release.assert_not_called()
    sqlite_lock.acquire.assert_not_called()


def test_crawl_congestion_skips_when_live_lock_held(monkeypatch):
    monkeypatch.setattr(
        scheduler,
        "LIVE_LOCK",
        MagicMock(acquire=MagicMock(return_value=False), release=MagicMock()),
    )
    # Live lock not acquired -> returns early without importing the crawler.
    scheduler.crawl_congestion_job()


def test_crawl_congestion_skips_when_sqlite_lock_contended(monkeypatch):
    monkeypatch.setattr(
        scheduler,
        "LIVE_LOCK",
        MagicMock(acquire=MagicMock(return_value=True), release=MagicMock()),
    )
    sqlite_lock = MagicMock()
    sqlite_lock.acquire.return_value = False
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", sqlite_lock)

    scheduler.crawl_congestion_job()

    sqlite_lock.acquire.assert_called_once_with(blocking=False, timeout=None)
    sqlite_lock.release.assert_not_called()


def test_sqlite_writer_lock_is_force_process_lock():
    from src.utils.lock import ForceProcessLock

    assert isinstance(scheduler.SQLITE_WRITE_LOCK, ForceProcessLock)
