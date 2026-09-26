"""Tests for the dead letter retry maintenance job."""

from __future__ import annotations

from unittest.mock import patch

from src.scheduler.jobs import maintenance
from src.services.crawl_dead_letter_worker import DlqWorkerSummary


class _NullLock:
    """Context manager that acquires nothing (test double for scheduler locks)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def test_nothing_due_returns_without_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    with (
        patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", return_value=DlqWorkerSummary()),
        patch("src.scheduler.jobs.maintenance.alert_warning") as mock_warn,
    ):
        maintenance.crawl_dead_letter_retry_job()
    mock_warn.assert_not_called()


def test_exhausted_triggers_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    summary = DlqWorkerSummary(attempted=2, resolved=1, exhausted=1)
    with (
        patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", return_value=summary),
        patch("src.scheduler.jobs.maintenance.alert_warning") as mock_warn,
    ):
        maintenance.crawl_dead_letter_retry_job()
    mock_warn.assert_called_once()


def test_resolved_only_does_not_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    summary = DlqWorkerSummary(attempted=1, resolved=1)
    with (
        patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", return_value=summary),
        patch("src.scheduler.jobs.maintenance.alert_warning") as mock_warn,
    ):
        maintenance.crawl_dead_letter_retry_job()
    mock_warn.assert_not_called()


def test_retry_job_is_registered_in_scheduler() -> None:
    from src.scheduler import registry

    assert hasattr(registry, "crawl_dead_letter_retry_job")
