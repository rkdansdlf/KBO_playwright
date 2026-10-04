"""A lock skip must report which lock timed out.

``_scheduler_job_lock`` raises ``_LockSkipped`` for two different reasons: the
tier lock, and -- only on SQLite deployments -- the writer lock. The guard's
warning named the sqlite writer unconditionally, so a ``MAINTENANCE_LOCK``
contention was reported as "sqlite_writer lock timed out". An operator following
that would go looking at ``SQLITE_WRITE_LOCK``, which the production PostgreSQL
deployment does not take at all: ``_scheduler_uses_sqlite_database()`` is false
there, so the writer lock is never acquired and cannot have timed out.

The cause now travels on the exception, so the warning names the lock that
actually blocked the job.
"""

from __future__ import annotations

import logging

import pytest

from src.scheduler import locks
from src.scheduler.locks import (
    LOCK_SKIP_SQLITE_WRITER,
    LOCK_SKIP_TIER,
    _LockSkipped,
    _scheduler_job_lock,
    _with_lock_skip_guard,
)


def _warning_for(cause: str, caplog: pytest.LogCaptureFixture) -> str:
    """Run one guarded job that raises ``_LockSkipped(cause)`` and return its warning."""

    @_with_lock_skip_guard
    def job() -> str:  # pragma: no cover - never reached
        raise _LockSkipped(cause)

    with caplog.at_level(logging.WARNING, logger="src.scheduler.locks"):
        assert job() is None
    return next(record.getMessage() for record in caplog.records if record.name == "src.scheduler.locks")


def test_a_tier_contention_is_not_reported_as_the_sqlite_writer(caplog: pytest.LogCaptureFixture) -> None:
    """The regression: a tier skip named a lock the job never took."""
    message = _warning_for(LOCK_SKIP_TIER, caplog)

    assert "tier lock" in message
    assert "sqlite_writer" not in message


def test_a_writer_contention_still_names_the_writer(caplog: pytest.LogCaptureFixture) -> None:
    """The SQLite case must keep the report it always had."""
    message = _warning_for(LOCK_SKIP_SQLITE_WRITER, caplog)

    assert "sqlite_writer lock" in message


def test_the_warning_never_names_the_exception_class(caplog: pytest.LogCaptureFixture) -> None:
    """``_LockSkipped`` in the log is a forbidden signature for an *escaped* skip.

    ``scripts/check_p1p2_lock_health.py`` greps the scheduler log for that string
    to detect a skip that was never handled. Naming the class in a handled skip
    would report every routine contention as a failure.
    """
    for cause in (LOCK_SKIP_TIER, LOCK_SKIP_SQLITE_WRITER):
        assert "_LockSkipped" not in _warning_for(cause, caplog)


def test_bare_raise_still_works_for_callers_that_do_not_report_a_cause() -> None:
    """Five test doubles and any future caller raise it without arguments."""
    assert _LockSkipped().cause == locks.LOCK_SKIP_UNSPECIFIED


def test_the_tier_path_carries_the_tier_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    """The raise site must actually label the tier lock, not just the guard."""

    class _Busy:
        name = "maintenance"

        def acquire(self, *_args: object, **_kwargs: object) -> bool:
            return False

        def release(self) -> None:
            pass

    with pytest.raises(_LockSkipped) as excinfo:
        with _scheduler_job_lock(_Busy()):  # type: ignore[arg-type]
            pass  # pragma: no cover - the lock is never granted

    assert excinfo.value.cause == LOCK_SKIP_TIER


def test_the_writer_path_carries_the_writer_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    """And the SQLite writer lock labels itself, not the tier lock."""
    monkeypatch.setattr(locks, "_scheduler_uses_sqlite_database", lambda: True)
    monkeypatch.setattr(locks, "_sqlite_writer_lock", lambda **_kwargs: _FalseCtx())

    class _Granted:
        name = "maintenance"

        def acquire(self, *_args: object, **_kwargs: object) -> bool:
            return True

        def release(self) -> None:
            pass

    with pytest.raises(_LockSkipped) as excinfo:
        with _scheduler_job_lock(_Granted()):  # type: ignore[arg-type]
            pass  # pragma: no cover - the writer lock is never granted

    assert excinfo.value.cause == LOCK_SKIP_SQLITE_WRITER


class _FalseCtx:
    """Stand-in for the writer-lock context manager that reports contention."""

    def __enter__(self) -> bool:
        return False

    def __exit__(self, *_exc: object) -> None:
        return None
