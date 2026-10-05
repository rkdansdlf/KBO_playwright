"""Pregame-refresh database gate tests.

Same outage as ``test_live_refresh_db_gate.py``, one job over. ``crawl_live_refresh``
got a ``database_reachable()`` probe in 8f1f8095; its 15-minute sibling
``crawl_pregame_refresh`` did not, even though it takes the same ``LIVE_LOCK`` and
reaches the database once per target date through ``_pregame_refresh_summary``.

The retry on ``_process_pregame_date`` does not cover this: ``_pregame_refresh_summary``
catches ``SCHEDULER_JOB_EXCEPTIONS`` and ``Exception``, so a database error is
swallowed inside the callee and never reaches tenacity.

Both jobs once probed inline, in a job body, before taking the lock. They now
declare ``@_with_db_fail_fast_guard`` like the other 23, which is what puts them
under the shared cooldown memo and under the single-location contract in
``test_db_fail_fast_jobs.py``. These tests therefore patch ``locks.database_reachable``
rather than an attribute of the bootstrap module: the probe the job reaches is the
decorator's, not the job's.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import scripts.scheduler as scheduler
import src.scheduler.jobs.live as live
from src.scheduler import locks

ORDER: list[str] = []


@pytest.fixture(autouse=True)
def _reset_order() -> None:
    ORDER.clear()


@pytest.fixture(autouse=True)
def _free_db_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start from a reachable gate; a test that wants otherwise says so.

    ``_DB_GATE`` memoises per URL set, so one test that made it fail would answer
    for the next, and the assertions below would pass because the job returned
    early rather than because it behaved.
    """
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    locks._reset_db_gate()
    yield
    locks._reset_db_gate()


def _lock_recorder() -> MagicMock:
    lock = MagicMock()
    lock.acquire.return_value = True

    def _acquire(*_args: object, **_kwargs: object) -> bool:
        ORDER.append("acquire")
        return True

    lock.acquire.side_effect = _acquire
    return lock


def _patch(monkeypatch: pytest.MonkeyPatch, *, reachable: bool, dates: list[str]) -> MagicMock:
    """Patch both namespaces the function actually resolves against.

    ``LIVE_LOCK`` comes from the bootstrap module (the function reads it with
    ``getattr(mod, ...)``, mirroring ``crawl_live_refresh``), the probe comes from
    the shared gate in ``locks``, and the helpers are plain globals of the defining
    module -- so patching ``scheduler.X`` would silently do nothing.
    """
    lock = _lock_recorder()
    monkeypatch.setattr(scheduler, "LIVE_LOCK", lock, raising=False)

    def _probe(**_kwargs: object) -> bool:
        ORDER.append("probe")
        return reachable

    monkeypatch.setattr(locks, "database_reachable", _probe)
    locks._reset_db_gate()
    monkeypatch.setattr(live, "_pregame_target_dates", lambda: dates)
    monkeypatch.setattr(live, "_process_pregame_date", lambda *_a, **_k: ORDER.append("process"))
    monkeypatch.setattr(live, "alert_success", lambda *_a, **_k: None)
    return lock


def test_unreachable_database_returns_before_taking_the_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreachable database must cost a fast return, not the connect timeout."""
    lock = _patch(monkeypatch, reachable=False, dates=["20260925"])

    scheduler.crawl_pregame_refresh()

    assert "process" not in ORDER
    lock.acquire.assert_not_called()


def test_probe_is_consulted_before_the_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """The probe has to precede acquire, or it buys nothing."""
    _patch(monkeypatch, reachable=False, dates=["20260925"])

    scheduler.crawl_pregame_refresh()

    assert ORDER == ["probe"]


def test_reachable_database_still_processes_every_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """The gate must not change behaviour while the database answers."""
    lock = _patch(monkeypatch, reachable=True, dates=["20260925", "20260926"])

    scheduler.crawl_pregame_refresh()

    assert ORDER.count("process") == 2
    lock.acquire.assert_called_once()


def test_lock_contention_still_short_circuits_when_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """The pre-existing LIVE_LOCK guard must keep working after the probe."""
    lock = _patch(monkeypatch, reachable=True, dates=["20260925"])
    lock.acquire.side_effect = None
    lock.acquire.return_value = False

    scheduler.crawl_pregame_refresh()

    assert "process" not in ORDER
