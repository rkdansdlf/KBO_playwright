"""Tests for the canonical scheduler selector-drift sentinel job.

The job lives in ``src/scheduler/jobs/sentinel.py``; ``scripts/scheduler.py`` is
only a bootstrap re-export, so these tests target the canonical module.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.scheduler.jobs.sentinel import SELECTOR_DRIFT_KEY, selector_drift_sentinel_job


def _healthy_report() -> MagicMock:
    report = MagicMock()
    report.is_healthy = True
    return report


def _drifted_report() -> MagicMock:
    report = MagicMock()
    report.is_healthy = False
    report.missing_selectors = [".tbl"]
    report.mismatched_columns = ["column mismatch"]
    return report


def _response(status_code: int = 200) -> MagicMock:
    return MagicMock(status_code=status_code, text="<html><table class='tbl'></table></html>")


def test_sentinel_job_healthy_recovers_incident() -> None:
    response = _response()
    sentinel = MagicMock()
    sentinel.check_html.return_value = _healthy_report()

    with (
        patch("src.scheduler.jobs.sentinel.requests.get", return_value=response),
        patch("src.scheduler.jobs.sentinel.apply_incidents") as apply,
        patch(
            "src.monitoring.selector_drift_sentinel.create_default_kbo_sentinel",
            return_value=sentinel,
        ),
    ):
        selector_drift_sentinel_job()

    sentinel.register_contract.assert_called_once()
    sentinel.check_html.assert_called_once_with("schedule", response.text)
    assert apply.call_args.args[0] == []
    assert apply.call_args.kwargs["resolve_keys"] == [SELECTOR_DRIFT_KEY]


def test_sentinel_job_drift_publishes_incident() -> None:
    response = _response()
    sentinel = MagicMock()
    sentinel.check_html.return_value = _drifted_report()

    with (
        patch("src.scheduler.jobs.sentinel.requests.get", return_value=response),
        patch("src.scheduler.jobs.sentinel.apply_incidents") as apply,
        patch(
            "src.monitoring.selector_drift_sentinel.create_default_kbo_sentinel",
            return_value=sentinel,
        ),
    ):
        selector_drift_sentinel_job()

    events = apply.call_args.args[0]
    assert len(events) == 1
    assert events[0].incident_key == SELECTOR_DRIFT_KEY
    assert events[0].severity.value == "ERROR"
    assert events[0].component == "selector:schedule"
    assert ".tbl" in events[0].message


def test_sentinel_job_non_200_skips_check() -> None:
    response = _response(status_code=503)

    with (
        patch("src.scheduler.jobs.sentinel.requests.get", return_value=response),
        patch("src.scheduler.jobs.sentinel.apply_incidents") as apply,
        patch(
            "src.monitoring.selector_drift_sentinel.create_default_kbo_sentinel",
            return_value=MagicMock(),
        ) as create,
    ):
        selector_drift_sentinel_job()

    create.return_value.check_html.assert_not_called()
    apply.assert_not_called()


def test_sentinel_job_fetch_error_is_non_blocking() -> None:
    with (
        patch("src.scheduler.jobs.sentinel.requests.get", side_effect=OSError("network down")),
        patch("src.scheduler.jobs.sentinel.logger") as logger,
    ):
        selector_drift_sentinel_job()

    logger.exception.assert_called()
