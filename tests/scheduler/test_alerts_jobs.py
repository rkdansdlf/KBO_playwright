"""Notification dispatch job tests.

Both dispatches previously ran only in the manually dispatched ``daily-extras`` job, so
the scheduled pipeline never sent them.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.scheduler.jobs import alerts


def _freeze_year(monkeypatch: pytest.MonkeyPatch, year: int) -> None:
    """Pin ``alerts.datetime`` so the season argument is observable."""
    frozen = datetime(year, 1, 1, tzinfo=None)

    class _FixedDateTime:
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return frozen.replace(tzinfo=tz)  # type: ignore[arg-type]

    monkeypatch.setattr(alerts, "datetime", _FixedDateTime)


@pytest.fixture(autouse=True)
def _assume_database_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """The jobs probe the operational database; keep that probe out of these tests."""
    monkeypatch.setattr(alerts, "database_reachable", lambda: True)


@pytest.fixture(autouse=True)
def _stub_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the real maintenance lock file out of the test."""
    monkeypatch.setattr(alerts, "_scheduler_job_lock", MagicMock())


def test_milestone_summary_dispatches_the_current_season(monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI defaults ``--season`` to a literal year, so it must be passed."""
    dispatched: list[tuple[str, tuple[str, ...]]] = []
    monkeypatch.setattr(alerts, "_dispatch", lambda module, *args: dispatched.append((module, args)))
    _freeze_year(monkeypatch, 2027)

    alerts.send_milestone_summary_job()

    assert dispatched == [
        ("src.cli.send_milestone_daily_summary", ("--season", "2027", "--channels", "telegram")),
    ]


def test_pregame_alerts_dispatch_the_pregame_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    dispatched: list[tuple[str, tuple[str, ...]]] = []
    monkeypatch.setattr(alerts, "_dispatch", lambda module, *args: dispatched.append((module, args)))
    _freeze_year(monkeypatch, 2027)

    alerts.send_pregame_alerts_job()

    assert dispatched == [
        ("src.cli.send_today_pregame_alerts", ("--season", "2027", "--channels", "telegram")),
    ]


def test_dispatch_is_skipped_before_taking_the_lock_when_db_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreachable database must cost a fast return, not a held lock and a timeout."""
    dispatched: list[str] = []
    lock = MagicMock()
    monkeypatch.setattr(alerts, "database_reachable", lambda: False)
    monkeypatch.setattr(alerts, "_dispatch", lambda module, *args: dispatched.append(module))
    monkeypatch.setattr(alerts, "_scheduler_job_lock", lock)

    alerts.send_pregame_alerts_job()
    alerts.send_milestone_summary_job()

    assert dispatched == []
    lock.assert_not_called()


def test_a_failing_dispatch_is_reported_not_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(module: str, *args: str) -> None:
        msg = "no transport"
        raise RuntimeError(msg)

    monkeypatch.setattr(alerts, "_dispatch", _boom)

    alerts.send_milestone_summary_job()  # must not raise
