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


def _patch_alerts():
    """Patch both alert channels; the real ``alert_success`` opens a session."""
    return (
        patch("src.scheduler.jobs.maintenance.alert_warning"),
        patch("src.scheduler.jobs.maintenance.alert_success"),
    )


def test_nothing_due_returns_without_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    warn_patch, success_patch = _patch_alerts()
    with (
        patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", return_value=DlqWorkerSummary()),
        warn_patch as mock_warn,
        success_patch as mock_success,
    ):
        maintenance.crawl_dead_letter_retry_job()
    mock_warn.assert_not_called()
    mock_success.assert_called_once()


def test_exhausted_triggers_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    summary = DlqWorkerSummary(attempted=2, resolved=1, exhausted=1)
    warn_patch, success_patch = _patch_alerts()
    with (
        patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", return_value=summary),
        warn_patch as mock_warn,
        success_patch as mock_success,
    ):
        maintenance.crawl_dead_letter_retry_job()
    mock_warn.assert_called_once()
    # A failing sweep must not also report recovery.
    mock_success.assert_not_called()


def test_resolved_only_does_not_alert(monkeypatch) -> None:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    summary = DlqWorkerSummary(attempted=1, resolved=1)
    warn_patch, success_patch = _patch_alerts()
    with (
        patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", return_value=summary),
        warn_patch as mock_warn,
        success_patch as mock_success,
    ):
        maintenance.crawl_dead_letter_retry_job()
    mock_warn.assert_not_called()
    mock_success.assert_called_once()


class TestAHealthySweepClearsTheStaleWarning:
    """``alert_warning`` opens ``scheduler:crawl_dead_letter_retry:warning``.

    Nothing else owns that key, so the run that finds nothing wrong is the only
    thing that can close it.
    """

    @staticmethod
    def _run(monkeypatch, summary: DlqWorkerSummary):
        monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
        warn_patch, success_patch = _patch_alerts()
        with (
            patch("src.services.crawl_dead_letter_worker.retry_due_dead_letters", return_value=summary),
            warn_patch as mock_warn,
            success_patch as mock_success,
        ):
            maintenance.crawl_dead_letter_retry_job()
        return mock_warn, mock_success

    def test_nothing_due_reports_recovery(self, monkeypatch) -> None:
        _warn, success = self._run(monkeypatch, DlqWorkerSummary())

        assert success.call_args.args[0] == "crawl_dead_letter_retry"

    def test_a_clean_batch_reports_recovery(self, monkeypatch) -> None:
        _warn, success = self._run(monkeypatch, DlqWorkerSummary(attempted=3, resolved=3))

        assert success.call_args.args[0] == "crawl_dead_letter_retry"

    def test_an_errored_batch_leaves_the_warning_open(self, monkeypatch) -> None:
        _warn, success = self._run(monkeypatch, DlqWorkerSummary(attempted=1, errored=1))

        success.assert_not_called()


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
