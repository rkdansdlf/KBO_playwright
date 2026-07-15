from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.utils.lock import ProcessLock

from scripts import scheduler


def test_crawl_congestion_skips_under_real_lock_contention(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A real held sqlite_writer lock must make congestion skip gracefully.

    Reproduces the original incident end-to-end with a file-backed lock held by
    another thread (not a mock), asserting the job returns without raising
    ``LockAcquisitionError`` and records a skip metric.
    """
    real_lock = ProcessLock("sqlite_writer", lock_dir=tmp_path, blocking=False)

    held = threading.Event()

    def holder() -> None:
        with real_lock:
            held.set()
            time.sleep(0.5)

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    assert held.wait(timeout=5)

    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", real_lock)
    monkeypatch.setattr(
        scheduler,
        "LIVE_LOCK",
        MagicMock(acquire=MagicMock(return_value=True), release=MagicMock()),
    )
    skip_metric = MagicMock()
    monkeypatch.setattr(scheduler, "KBO_SCHEDULER_LOCK_SKIP_TOTAL", skip_metric)

    scheduler.crawl_congestion_job()

    holder_thread.join(timeout=5)

    skip_metric.labels.assert_any_call(job_id="crawl_congestion", lock="sqlite_writer")
    skip_metric.labels.return_value.inc.assert_called()


def test_sqlite_writer_lock_yields_true_when_real_lock_free(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """When no other job holds sqlite_writer, the helper yields True (proceed)."""
    real_lock = ProcessLock("sqlite_writer_free", lock_dir=tmp_path, blocking=True)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", real_lock)

    with scheduler._sqlite_writer_lock(blocking=True) as acquired:
        assert acquired is True


def test_acquire_times_out_when_held_by_other_thread(tmp_path: Path) -> None:
    """A blocking acquire must give up after ``timeout`` when another thread holds it."""
    lock = ProcessLock("test_timeout", lock_dir=tmp_path, blocking=True)

    held = threading.Event()

    def holder() -> None:
        with lock:
            held.set()
            time.sleep(1.0)

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    assert held.wait(timeout=5)

    start = time.monotonic()
    acquired = lock.acquire(timeout=0.2)
    elapsed = time.monotonic() - start

    assert acquired is False
    assert 0.1 <= elapsed <= 0.9

    holder_thread.join(timeout=5)


def test_scheduler_job_lock_raises_skip_on_sqlite_timeout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """_scheduler_job_lock raises _LockSkipped when sqlite_writer times out."""
    real_lock = ProcessLock("sqlite_writer", lock_dir=tmp_path, blocking=True)

    held = threading.Event()

    def holder() -> None:
        with real_lock:
            held.set()
            time.sleep(1.0)

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    assert held.wait(timeout=5)

    monkeypatch.setenv("DATABASE_URL", "sqlite:///data/kbo_dev.db")
    monkeypatch.setattr(scheduler, "SQLITE_WRITE_LOCK", real_lock)
    tier = MagicMock(acquire=MagicMock(return_value=True), release=MagicMock())

    with pytest.raises(scheduler._LockSkipped):
        with scheduler._scheduler_job_lock(tier, sqlite_timeout=0.2):
            pass

    holder_thread.join(timeout=5)


def test_with_lock_skip_guard_suppresses_skip(monkeypatch) -> None:
    """The guard decorator converts a _LockSkipped into a silent None return."""
    calls: list[str] = []

    @scheduler._with_lock_skip_guard
    def job() -> str:
        calls.append("entered")
        raise scheduler._LockSkipped

    assert job() is None
    assert calls == ["entered"]


def test_tier_job_skips_on_lock_timeout(monkeypatch) -> None:
    """A decorated tier job skips silently when its lock times out."""

    @contextmanager
    def _raising_lock(*_args, **_kwargs):
        raise scheduler._LockSkipped
        yield

    monkeypatch.setattr(scheduler, "_scheduler_job_lock", _raising_lock)

    assert scheduler.crawl_operation_notices_job() is None
