"""Tests for the canonical scheduler selector-drift sentinel job.

The job lives in ``src/scheduler/jobs/sentinel.py``; ``scripts/scheduler.py`` is
only a bootstrap re-export, so these tests target the canonical module.

The job's only output is an incident, so the DB gate decides whether looking is
even worthwhile: ``apply_incidents`` contains its own persistence errors, which
means an ungated run during an outage would fetch the page, reach a verdict, and
drop it without saying so.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from src.scheduler import locks
from src.scheduler.jobs.sentinel import SELECTOR_DRIFT_KEY, selector_drift_sentinel_job

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _reachable_gate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep the DB fail-fast gate out of these tests.

    The job is gated now and the gate probes for real, so leaving it live would
    make every assertion depend on the test database answering -- and a memoised
    failure from another test could turn the whole file into vacuous passes. The
    gate's own behaviour is tested in ``test_db_fail_fast_jobs``.
    """
    monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
    locks._reset_db_gate()
    yield
    locks._reset_db_gate()


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


class TestTheGateRunsFirst:
    """A detection that leaves no trace is worse than one that never happened.

    ``apply_incidents`` logs its persistence failures and returns normally, so an
    ungated run during an outage would fetch the page, find real drift, and drop
    it -- while the scheduler log still says the canary ran. The page request is
    the thing worth skipping: it is a live network call whose only product would
    be an incident nobody can read.
    """

    def test_a_dead_database_skips_before_the_page_is_fetched(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: False)
        locks._reset_db_gate()

        with (
            patch("src.scheduler.jobs.sentinel.requests.get") as get,
            patch("src.scheduler.jobs.sentinel.apply_incidents") as apply,
        ):
            selector_drift_sentinel_job()

        get.assert_not_called()
        apply.assert_not_called()

    def test_a_dead_database_never_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The gate is silent, so tenacity and the failure alarm stay out of it."""
        monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: False)
        locks._reset_db_gate()

        selector_drift_sentinel_job()

    def test_a_reachable_database_still_checks_the_page(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The gate must not be a mute button: recovery is picked up immediately.

        The probe memoises failures only, so a run after the outage reaches the
        same code path as before the gate existed.
        """
        monkeypatch.setattr(locks, "database_reachable", lambda **_kwargs: True)
        sentinel = MagicMock()
        sentinel.check_html.return_value = _healthy_report()

        with (
            patch("src.scheduler.jobs.sentinel.requests.get", return_value=_response()) as get,
            patch("src.scheduler.jobs.sentinel.apply_incidents") as apply,
            patch(
                "src.monitoring.selector_drift_sentinel.create_default_kbo_sentinel",
                return_value=sentinel,
            ),
        ):
            selector_drift_sentinel_job()

        get.assert_called_once()
        apply.assert_called_once()
