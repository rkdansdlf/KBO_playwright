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


def test_batch_failure_is_retried_by_tenacity(monkeypatch) -> None:
    """A batch-level failure must propagate so @retry actually retries (3 attempts)."""
    from sqlalchemy.exc import SQLAlchemyError

    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("src.scheduler.alerting.apply_incidents", lambda *_a, **_k: None)

    calls: list[int] = []

    def _boom(*_args: object, **_kwargs: object) -> object:
        calls.append(1)
        raise SQLAlchemyError("db down")

    with patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", _boom):
        maintenance.crawl_dead_letter_retry_job()

    assert len(calls) == 3


def test_batch_failure_is_retried_by_tenacity_recovery(monkeypatch) -> None:
    from sqlalchemy.exc import SQLAlchemyError

    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("src.scheduler.alerting.apply_incidents", lambda *_a, **_k: None)

    calls: list[int] = []

    def _boom(*_args: object, **_kwargs: object) -> object:
        calls.append(1)
        raise SQLAlchemyError("db down")

    with patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", _boom):
        maintenance.crawl_dead_letter_recovery_job()

    assert len(calls) == 3
