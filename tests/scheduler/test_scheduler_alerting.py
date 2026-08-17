"""Unit tests for src/scheduler/alerting.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.scheduler.alerting import (
    alert_failure,
    alert_success,
    alert_warning,
)


def test_alert_failure():
    mock_retry_state = MagicMock()
    mock_retry_state.fn.__name__ = "test_func"
    mock_retry_state.attempt_number = 3
    mock_retry_state.outcome.exception.return_value = RuntimeError("Crash")

    with patch("src.utils.alerting.SlackWebhookClient.send_error_alert") as mock_slack:
        alert_failure(mock_retry_state)
        mock_slack.assert_called_once()
        assert "test_func" in mock_slack.call_args[0][0]


def test_alert_warning():
    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_slack:
        alert_warning("warn_job", "some warning")
        mock_slack.assert_called_once()
        assert "warn_job" in mock_slack.call_args[0][0]


def test_alert_success():
    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_slack:
        alert_success("success_job", "finished")
        mock_slack.assert_called_once()
        assert "success_job" in mock_slack.call_args[0][0]
