"""Tests for the scheduler selector-drift sentinel job (scripts/scheduler.py)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "scheduler.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("scheduler_sentinel_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _healthy_report():
    report = MagicMock()
    report.is_healthy = True
    return report


def _drifted_report():
    report = MagicMock()
    report.is_healthy = False
    report.missing_selectors = [".tbl"]
    report.mismatched_columns = ["column mismatch"]
    return report


def test_sentinel_job_healthy_no_alert():
    sched = _load_module()
    response = MagicMock()
    response.status_code = 200
    response.text = "<html><table class='tbl'></table></html>"
    with (
        patch("requests.get", return_value=response),
        patch(
            "src.monitoring.selector_drift_sentinel.create_default_kbo_sentinel",
            return_value=MagicMock(),
        ) as create,
        patch.object(sched, "SlackWebhookClient") as alert_client,
    ):
        sentinel = create.return_value
        sentinel.check_html.return_value = _healthy_report()
        sched.selector_drift_sentinel_job()
        sentinel.register_contract.assert_called_once()
        sentinel.check_html.assert_called_once_with("schedule", response.text)
        alert_client.send_alert.assert_not_called()


def test_sentinel_job_drift_alerts():
    sched = _load_module()
    response = MagicMock()
    response.status_code = 200
    response.text = "<html><table></table></html>"
    sentinel = MagicMock()
    sentinel.check_html.return_value = _drifted_report()
    with (
        patch("requests.get", return_value=response),
        patch(
            "src.monitoring.selector_drift_sentinel.create_default_kbo_sentinel",
            return_value=sentinel,
        ),
        patch("src.utils.alerting.SlackWebhookClient") as alert_client,
    ):
        sched.selector_drift_sentinel_job()
        sentinel.register_contract.assert_called_once()
        sentinel.check_html.assert_called_once_with("schedule", response.text)
        alert_client.send_alert.assert_called_once()
        assert ".tbl" in alert_client.send_alert.call_args.args[0]


def test_sentinel_job_non_200_skips_check():
    sched = _load_module()
    response = MagicMock()
    response.status_code = 503
    with (
        patch("requests.get", return_value=response),
        patch(
            "src.monitoring.selector_drift_sentinel.create_default_kbo_sentinel",
            return_value=MagicMock(),
        ) as create,
        patch.object(sched, "SlackWebhookClient") as alert_client,
    ):
        sched.selector_drift_sentinel_job()
        create.return_value.check_html.assert_not_called()
        alert_client.send_alert.assert_not_called()


def test_sentinel_job_fetch_error_is_non_blocking():
    sched = _load_module()
    with (
        patch("requests.get", side_effect=OSError("network down")),
        patch.object(sched, "logger") as logger,
    ):
        sched.selector_drift_sentinel_job()
        logger.exception.assert_called()
