"""Unit tests for src/scheduler/alerting.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.scheduler.alerting import (
    alert_failure,
    alert_success,
    alert_warning,
)


def test_alert_failure_publishes_failed_incident():
    mock_retry_state = MagicMock()
    mock_retry_state.fn.__name__ = "test_func"
    mock_retry_state.attempt_number = 3
    mock_retry_state.outcome.exception.return_value = RuntimeError("Crash")

    with patch("src.scheduler.alerting.apply_incidents") as mock_apply:
        alert_failure(mock_retry_state)

    events = mock_apply.call_args.args[0]
    assert len(events) == 1
    assert events[0].incident_key == "scheduler:test_func:failed"
    assert events[0].component == "test_func"
    assert "Crash" in events[0].message


def test_alert_warning_publishes_warning_incident():
    with patch("src.scheduler.alerting.apply_incidents") as mock_apply:
        alert_warning("warn_job", "some warning")

    events = mock_apply.call_args.args[0]
    assert len(events) == 1
    assert events[0].incident_key == "scheduler:warn_job:warning"
    assert events[0].component == "warn_job"
    assert "some warning" in events[0].message


def test_alert_success_resolves_incidents(monkeypatch):
    monkeypatch.setenv("NOTIFY_SUCCESS", "1")
    with patch("src.scheduler.alerting.apply_incidents") as mock_apply:
        alert_success("success_job", "finished")

    assert mock_apply.call_args.args[0] == []
    assert mock_apply.call_args.kwargs["resolve_keys"] == [
        "scheduler:success_job:warning",
        "scheduler:success_job:failed",
    ]
    assert mock_apply.call_args.kwargs["dry_run"] is False
