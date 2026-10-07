"""Invariants distinguishing a partial run from a complete one.

A partial run is a *usable completion*, not a full success. It kept the
crawler alive and produced a payload, so liveness freshness may advance; but
it is not the same health state as a full success, and the two must stay
separable in observation. These tests pin that separation without pinning any
particular metric layout -- adding a dedicated degradation signal is allowed,
conflating partial with success is not.

The corresponding alert-rule coverage is checked by
:class:`TestSustainedPartialMustBeDetectable`.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from prometheus_client import REGISTRY

from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm

_FINISHED = datetime(2026, 1, 8, 0, 2, 0)


def _run(status: str = "success", *, crawler: str = "awards", records_written: int = 5) -> CrawlExecutionRun:
    return CrawlExecutionRun(
        run_id=f"probe-{status}-{crawler}-{records_written}",
        crawler=crawler,
        target_type="award_history",
        status=status,
        started_at=datetime(2026, 1, 8, 0, 0, 0),
        finished_at=_FINISHED,
        records_read=10,
        records_written=records_written,
        records_failed=0,
    )


def _sample(name: str, **labels: object) -> float | None:
    return REGISTRY.get_sample_value(name, labels)


@pytest.fixture(autouse=True)
def _reset_crawler_series() -> None:
    for metric in (
        cm.CRAWL_RUNS_TOTAL,
        cm.CRAWL_FAILURES_TOTAL,
        cm.CRAWL_LAST_SUCCESS_TIMESTAMP,
        cm.CRAWL_RECORDS_WRITTEN_LAST,
    ):
        metric.clear()
    cm.reset_initialized_crawlers()
    yield
    for metric in (
        cm.CRAWL_RUNS_TOTAL,
        cm.CRAWL_FAILURES_TOTAL,
        cm.CRAWL_LAST_SUCCESS_TIMESTAMP,
        cm.CRAWL_RECORDS_WRITTEN_LAST,
    ):
        metric.clear()
    cm.reset_initialized_crawlers()


class TestPartialIsSeparableFromSuccess:
    """INV-METRIC-01: a partial run must be observationally distinguishable."""

    def test_a_partial_run_gets_its_own_series(self) -> None:
        cm.record_crawl_run(_run("partial", crawler="sep", records_written=5))

        assert _sample("kbo_crawl_runs_total", crawler="sep", status="partial") == 1.0
        # A success-only series must not appear just because a partial ran:
        # otherwise "did it fully succeed?" has no answer.
        assert _sample("kbo_crawl_runs_total", crawler="sep", status="success") is None

    def test_the_two_statuses_do_not_share_a_counter(self) -> None:
        """Both statuses cannot collapse onto one label value."""
        cm.record_crawl_run(_run("success", crawler="sep2", records_written=5))
        cm.record_crawl_run(_run("partial", crawler="sep2", records_written=5))

        assert _sample("kbo_crawl_runs_total", crawler="sep2", status="success") == 1.0
        assert _sample("kbo_crawl_runs_total", crawler="sep2", status="partial") == 1.0


class TestSinglePartialIsNotAnOutage:
    """INV-METRIC-03: one partial must not read as a crawler failure."""

    def test_a_partial_does_not_increment_the_failure_counter(self) -> None:
        cm.record_crawl_run(_run("partial", crawler="outage", records_written=5))

        assert _sample("kbo_crawl_failures_total", crawler="outage") is None

    def test_a_failed_run_does_increment_it(self) -> None:
        """The counterpart, so the previous test is not vacuously true."""
        cm.record_crawl_run(_run("failed", crawler="outage2", records_written=0))

        # The counter is labelled by crawler, error_code and failure_stage; a run
        # with no error_code falls back to the taxonomy's "unknown" pair.
        assert (
            _sample(
                "kbo_crawl_failures_total",
                crawler="outage2",
                error_code="UNKNOWN",
                failure_stage="unknown",
            )
            == 1.0
        )


class TestLivenessSurvivesAPartialRun:
    """A partial produced a payload, so liveness freshness may advance.

    This is the *conservative* operating choice, and it is asserted on purpose:
    the existing ``KboCrawlerNoRecentSuccess`` critical is expected to keep
    treating a partially-successful crawler as alive. Changing that is a
    separate, blast-radius-measured decision, not a side effect of this module.
    """

    def test_a_partial_run_advances_last_success(self) -> None:
        cm.record_crawl_run(_run("partial", crawler="live", records_written=5))

        expected = _FINISHED.replace(tzinfo=UTC).timestamp()
        assert _sample("kbo_crawl_last_success_timestamp", crawler="live") == pytest.approx(expected)

    def test_a_failed_run_does_not_advance_it(self) -> None:
        """A partial advancing freshness is only defensible if failure does not."""
        cm.record_crawl_run(_run("failed", crawler="dead", records_written=0))

        assert _sample("kbo_crawl_last_success_timestamp", crawler="dead") in (None, 0.0)
