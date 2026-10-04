"""A tier-lock skip must not be retried, and must not raise a failure alert.

Reproduces the defect measured on 2026-10-04. With ``MAINTENANCE_LOCK`` held by
another process, ``_scheduler_job_lock`` raises ``_LockSkipped``, whose documented
contract is "release the tier lock, log the skip, and let the scheduler retry on
the next cycle".

The three jobs that also carry a tenacity ``@retry`` did not honour it. The
decorator stack is ``_with_lock_skip_guard -> @retry -> _with_db_fail_fast_guard``,
so the skip signal was trapped inside the retry:

- the body ran 3 times with 120s/240s of backoff between them,
- ``retry_error_callback=alert_failure`` fired, and its incident write went to the
  same database that was contended,
- tenacity's error callback consumes the exception, so ``_with_lock_skip_guard``
  outside it never saw the skip and its clean warning was never logged.

Measured body attempts before the fix, all three jobs: 3.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pytest
from tenacity import wait_none

import src.scheduler.jobs.maintenance as maintenance
from src.scheduler import locks
from src.scheduler.locks import _LockSkipped

if TYPE_CHECKING:
    from collections.abc import Iterator

#: Jobs that carry both a tier lock and a tenacity retry.
RETRIED_LOCKED_JOBS = (
    "crawl_retired_players_job",
    "crawl_dead_letter_recovery_job",
    "crawl_dead_letter_retry_job",
)


@pytest.fixture(autouse=True)
def _gate_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report the database as reachable so the DB gate is not what is measured."""
    locks._reset_db_gate()
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)


@pytest.fixture
def busy_lock(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Make every tier-lock acquisition report contention, and count the attempts."""
    attempts: list[str] = []

    @contextmanager
    def _busy(*_args: object, **_kwargs: object) -> Iterator[None]:
        attempts.append("acquire")
        raise _LockSkipped("tier lock is held by another process")
        yield  # pragma: no cover - unreachable, keeps this a context manager

    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _busy)
    return attempts


@pytest.fixture
def alerts(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record failure alerts instead of dispatching them, and drop the backoff.

    ``alert_failure`` writes to the incident ledger, which lives in the database
    under contention; letting it run would turn this test into a 10s connect
    timeout per case and assert nothing useful. The backoff is zeroed for the
    same reason -- the point is how many times the body runs, not how long
    tenacity waits between them.
    """
    fired: list[str] = []
    for name in RETRIED_LOCKED_JOBS:
        job = getattr(maintenance, name)
        if getattr(job, "retry", None) is not None:
            monkeypatch.setattr(job.retry, "wait", wait_none())
            monkeypatch.setattr(job.retry, "retry_error_callback", lambda *a, **k: fired.append("alert"))
    return fired


@pytest.mark.parametrize("name", RETRIED_LOCKED_JOBS)
def test_a_lock_skip_is_not_retried(
    name: str,
    busy_lock: list[str],
    alerts: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One attempt, no failure alert, and the skip logged as a clean warning."""
    caplog.set_level(logging.WARNING, logger="src.scheduler.locks")

    with caplog.at_level(logging.WARNING, logger="src.scheduler.locks"):
        assert getattr(maintenance, name)() is None

    assert len(busy_lock) == 1, "a contended tier lock was retried instead of skipped"
    assert alerts == [], "routine lock contention raised a failure alert"
    assert any("skipped" in record.message for record in caplog.records), (
        "_with_lock_skip_guard never saw the skip, so its warning was lost"
    )
