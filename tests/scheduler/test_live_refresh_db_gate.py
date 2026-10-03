"""Live-refresh database gate tests.

Regression for the 2026-10-03 outage: with the database unreachable, the 10s live
refresh spent the connect timeout inside its interval query while holding its
``max_instances=1`` slot, so every following tick misfired and the job read as dead
for hours while the tier lock stayed occupied.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import scripts.scheduler as scheduler


@pytest.fixture(autouse=True)
def _reset_live_state() -> None:
    scheduler.LAST_LIVE_RUN_TIME = None
    scheduler.LAST_LIVE_POLL_INTERVAL = None


def _cycle_recorder() -> tuple[list[dict], object]:
    calls: list[dict] = []

    async def fake_cycle(**kwargs: object) -> dict:
        calls.append(kwargs)
        return {}

    return calls, fake_cycle


def test_unreachable_database_returns_before_taking_the_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreachable database must cost a fast return, not the connect timeout."""
    calls, fake_cycle = _cycle_recorder()
    live_lock = MagicMock()
    live_lock.acquire.return_value = True

    monkeypatch.setattr(scheduler, "LIVE_LOCK", live_lock)
    monkeypatch.setattr(scheduler, "database_reachable", lambda: False, raising=False)
    monkeypatch.setattr(scheduler, "_should_skip_live_for_pregame", lambda: False)
    monkeypatch.setattr(scheduler, "_get_live_poll_interval_seconds", lambda: 0)
    monkeypatch.setattr(scheduler, "run_live_crawler_cycle", fake_cycle)
    monkeypatch.setattr(scheduler, "_live_refresh_max_games_per_cycle", lambda: 1)

    scheduler.crawl_live_refresh()

    assert calls == []
    live_lock.acquire.assert_not_called()


def test_reachable_database_still_runs_the_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    """The gate must not change behaviour while the database answers."""
    calls, fake_cycle = _cycle_recorder()
    live_lock = MagicMock()
    live_lock.acquire.return_value = True

    monkeypatch.setattr(scheduler, "LIVE_LOCK", live_lock)
    monkeypatch.setattr(scheduler, "database_reachable", lambda: True, raising=False)
    monkeypatch.setattr(scheduler, "_should_skip_live_for_pregame", lambda: False)
    monkeypatch.setattr(scheduler, "_get_live_poll_interval_seconds", lambda: 0)
    monkeypatch.setattr(scheduler, "run_live_crawler_cycle", fake_cycle)
    monkeypatch.setattr(scheduler, "_live_refresh_max_games_per_cycle", lambda: 1)

    scheduler.crawl_live_refresh()

    assert len(calls) == 1
    live_lock.acquire.assert_called_once()
