"""Tests for the dead letter recovery maintenance job."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from src.scheduler.jobs import maintenance


class _NullLock:
    """Context manager that acquires nothing (test double for scheduler locks)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def _result(action: str) -> SimpleNamespace:
    return SimpleNamespace(action=action)


def test_nothing_stuck_returns_without_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    with (
        patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", return_value=[]),
        patch("src.scheduler.jobs.maintenance.alert_warning") as mock_warn,
    ):
        maintenance.crawl_dead_letter_recovery_job()
    mock_warn.assert_not_called()


def test_exhausted_triggers_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    results = [_result("exhausted"), _result("resolved")]
    with (
        patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", return_value=results),
        patch("src.scheduler.jobs.maintenance.alert_warning") as mock_warn,
    ):
        maintenance.crawl_dead_letter_recovery_job()
    mock_warn.assert_called_once()
    assert "exhausted=1" in mock_warn.call_args.args[1]


def test_resolved_only_does_not_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    with (
        patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", return_value=[_result("resolved")]),
        patch("src.scheduler.jobs.maintenance.alert_warning") as mock_warn,
    ):
        maintenance.crawl_dead_letter_recovery_job()
    mock_warn.assert_not_called()


def test_recovery_job_is_registered_in_scheduler() -> None:
    from src.scheduler import registry

    assert hasattr(registry, "crawl_dead_letter_recovery_job")
