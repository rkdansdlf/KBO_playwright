"""Tests for the weekly stadiums/places verification job."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.scheduler import locks


class _NullLock:
    """Context manager that acquires nothing (test double for scheduler locks)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


@pytest.fixture(autouse=True)
def _reachable_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the DB fail-fast gate out of these tests."""
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    locks._reset_db_gate()
    yield
    locks._reset_db_gate()


def _run(monkeypatch, code: int):
    from src.scheduler.jobs import maintenance

    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    with (
        patch("scripts.verify_stadiums_places.run", return_value=code) as mock_run,
        patch("src.scheduler.jobs.maintenance.alert_warning") as warn,
    ):
        maintenance.verify_stadiums_places_job()
    return mock_run, warn


def test_verify_pass_logs_without_alert(monkeypatch) -> None:
    mock_run, warn = _run(monkeypatch, 0)
    assert mock_run.called
    warn.assert_not_called()


def test_verify_failure_alerts(monkeypatch) -> None:
    _mock_run, warn = _run(monkeypatch, 1)
    warn.assert_called_once()
    assert warn.call_args.args[0] == "verify_stadiums_places"
