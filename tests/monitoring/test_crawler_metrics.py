"""Prometheus projection of crawl runs.

The ledger is the durable record; these metrics are a queryable projection of
it. The tests therefore check two things: that a terminal run moves the right
series, and that the projection cannot create unbounded cardinality.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from prometheus_client import REGISTRY

from src.monitoring import crawler_metrics as cm


def _sample(name: str, **labels: str) -> float:
    """Read a single sample from the default registry."""
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _Delta:
    """Measure the change a run caused in a series.

    The Prometheus registry is process-global, so a test cannot assert an
    absolute value without depending on whatever ran before it. Comparing the
    before and after value keeps each test independent of its neighbours.
    """

    def __init__(self, name: str, **labels: str) -> None:
        self.name = name
        self.labels = labels
        self.before = _sample(name, **labels)

    def after(self) -> float:
        """Return how much the series moved."""
        return _sample(self.name, **self.labels) - self.before


@pytest.fixture(autouse=True)
def _isolated_crawler_state():
    """Give each test the state a fresh process would have.

    The Prometheus registry is process-global, so a gauge left at 100 by an
    earlier test would make a later write-drop assertion pass or fail for the
    wrong reason.
    """
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


def _run(**overrides: object):
    """Build a terminal `CrawlExecutionRun`-shaped object."""

    class _Run:
        crawler = "awards"
        target_type = "award"
        status = "success"
        records_read = 0
        records_written = 0
        records_failed = 0
        error_code = None
        started_at = datetime(2026, 1, 8, 0, 0, 0)
        finished_at = datetime(2026, 1, 8, 0, 0, 30)

    run = _Run()
    for key, value in overrides.items():
        setattr(run, key, value)
    return run


class TestSuccessRun:
    def test_successful_run_increments_the_run_counter(self):
        delta = _Delta("kbo_crawl_runs_total", crawler="awards", status="success")

        assert cm.record_crawl_run(_run()) is True
        assert delta.after() == 1.0

    def test_record_counters_accumulate(self):
        read = _Delta("kbo_crawl_records_read_total", crawler="awards")
        written = _Delta("kbo_crawl_records_written_total", crawler="awards")
        failed = _Delta("kbo_crawl_records_failed_total", crawler="awards")

        cm.record_crawl_run(_run(records_read=100, records_written=90, records_failed=2))
        cm.record_crawl_run(_run(records_read=50, records_written=40, records_failed=1))

        assert read.after() == 150.0
        assert written.after() == 130.0
        assert failed.after() == 3.0

    def test_duration_is_observed(self):
        count = _Delta("kbo_crawl_duration_seconds_count", crawler="awards")
        total = _Delta("kbo_crawl_duration_seconds_sum", crawler="awards")

        cm.record_crawl_run(
            _run(
                started_at=datetime(2026, 1, 8, 0, 0, 0),
                finished_at=datetime(2026, 1, 8, 0, 2, 0),
            )
        )

        assert count.after() == 1.0
        # A Histogram sum is a float accumulated across every prior run, so an
        # exact equality here is only stable while this test is the only writer.
        assert total.after() == pytest.approx(120.0)

    def test_last_success_is_set_to_the_finish_time(self):
        finished = datetime(2026, 1, 8, 1, 2, 3)
        cm.record_crawl_run(_run(finished_at=finished))

        expected = finished.replace(tzinfo=UTC).timestamp()
        assert _sample("kbo_crawl_last_success_timestamp", crawler="awards") == pytest.approx(expected)

    def test_last_written_tracks_the_most_recent_run(self):
        cm.record_crawl_run(_run(records_written=90))
        cm.record_crawl_run(_run(records_written=12))

        assert _sample("kbo_crawl_records_written_last", crawler="awards") == 12.0

    def test_partial_counts_as_success_for_freshness(self):
        """A partial run still produced data, so freshness should be satisfied."""
        cm.record_crawl_run(_run(status="partial", records_written=5))

        assert _sample("kbo_crawl_runs_total", crawler="awards", status="partial") >= 1.0
        assert _sample("kbo_crawl_last_success_timestamp", crawler="awards") > 0.0


class TestFailedRun:
    def test_failure_is_classified_by_stage(self):
        delta = _Delta(
            "kbo_crawl_failures_total",
            crawler="awards",
            error_code="FETCH_RATE_LIMITED",
            failure_stage="fetch",
        )
        cm.record_crawl_run(_run(status="failed", error_code="FETCH_RATE_LIMITED"))
        assert delta.after() == 1.0

    def test_selector_drift_is_classified_as_a_parse_failure(self):
        delta = _Delta(
            "kbo_crawl_failures_total",
            crawler="awards",
            error_code="PARSE_SELECTOR_MISSING",
            failure_stage="parse",
        )
        cm.record_crawl_run(_run(status="failed", error_code="PARSE_SELECTOR_MISSING"))
        assert delta.after() == 1.0

    def test_unknown_error_code_does_not_raise(self):
        """`error_code` is a free-form column, so an unrecognised value is normal."""
        assert cm.record_crawl_run(_run(status="failed", error_code="SOMETHING_NEW")) is True

        delta = _Delta(
            "kbo_crawl_failures_total",
            crawler="awards",
            error_code="SOMETHING_NEW",
            failure_stage="unknown",
        )
        cm.record_crawl_run(_run(status="failed", error_code="SOMETHING_NEW"))
        assert delta.after() == 1.0

    def test_missing_error_code_falls_back_to_unknown(self):
        delta = _Delta(
            "kbo_crawl_failures_total",
            crawler="awards",
            error_code="UNKNOWN",
            failure_stage="unknown",
        )
        cm.record_crawl_run(_run(status="failed", error_code=None))
        assert delta.after() == 1.0


class TestNeverSucceededCrawlerIsVisible:
    """A freshness rule needs the series to exist, or it stays silent forever."""

    def test_failed_first_run_still_exposes_a_zero_freshness_gauge(self):
        cm.record_crawl_run(_run(crawler="brand_new", status="failed", error_code="FETCH_TIMEOUT"))

        assert _sample("kbo_crawl_last_success_timestamp", crawler="brand_new") == 0.0

    def test_never_successful_crawler_also_exposes_the_write_gauge(self):
        """The write-drop rule needs a baseline of 0, not a missing series."""
        cm.record_crawl_run(_run(crawler="brand_new", status="failed", error_code="FETCH_TIMEOUT"))

        assert _sample("kbo_crawl_records_written_last", crawler="brand_new") == 0.0

    def test_a_later_success_replaces_the_zero(self):
        cm.record_crawl_run(_run(crawler="brand_new", status="failed", error_code="FETCH_TIMEOUT"))
        cm.record_crawl_run(_run(crawler="brand_new", status="success", records_written=4))

        assert _sample("kbo_crawl_last_success_timestamp", crawler="brand_new") > 0.0
        assert _sample("kbo_crawl_records_written_last", crawler="brand_new") == 4.0


class TestCardinalityIsBounded:
    """A caller mistake must not turn one series per request."""

    @pytest.mark.parametrize(
        "label",
        [
            "https://stat.koreabaseball.com/api/Schedule",
            "src/crawlers/award_crawler.py",
            "a" * 200,
            "",
            "has space",
            "semi;colon",
        ],
    )
    def test_unusable_crawler_label_is_refused(self, label: str):
        assert cm.record_crawl_run(_run(crawler=label)) is False

    def test_refused_run_emits_nothing(self):
        cm.record_crawl_run(_run(crawler="https://example.invalid/x"))

        assert _sample("kbo_crawl_runs_total", crawler="https://example.invalid/x", status="success") == 0.0

    @pytest.mark.parametrize("label", ["awards", "team_events", "kbo.awards.v2", "crawl-1", "a_b:c"])
    def test_ordinary_crawler_names_are_accepted(self, label: str):
        assert cm.record_crawl_run(_run(crawler=label)) is True

    def test_non_string_crawler_is_refused(self):
        assert cm.record_crawl_run(_run(crawler=None)) is False

    def test_target_id_and_source_url_are_not_labels(self):
        """They live in the ledger; on a metric they would be one series per run."""
        for metric in (
            cm.CRAWL_RUNS_TOTAL,
            cm.CRAWL_FAILURES_TOTAL,
            cm.CRAWL_RECORDS_READ_TOTAL,
            cm.CRAWL_RECORDS_WRITTEN_TOTAL,
            cm.CRAWL_RECORDS_FAILED_TOTAL,
            cm.CRAWL_DURATION_SECONDS,
            cm.CRAWL_LAST_SUCCESS_TIMESTAMP,
            cm.CRAWL_RECORDS_WRITTEN_LAST,
        ):
            labels = set(metric._labelnames)
            assert "run_id" not in labels, metric
            assert "target_id" not in labels, metric
            assert "source_url" not in labels, metric
            assert "target_type" not in labels, metric
            assert "parser_version" not in labels, metric


class TestDegenerateInput:
    def test_missing_finish_time_skips_the_duration(self):
        count = _Delta("kbo_crawl_duration_seconds_count", crawler="unfinished")

        cm.record_crawl_run(_run(crawler="unfinished", finished_at=None))

        assert count.after() == 0.0

    def test_negative_record_counts_are_clamped(self):
        read = _Delta("kbo_crawl_records_read_total", crawler="neg_crawler")
        failed = _Delta("kbo_crawl_records_failed_total", crawler="neg_crawler")

        cm.record_crawl_run(_run(crawler="neg_crawler", records_read=-5, records_written=-5, records_failed=-5))

        assert read.after() == 0.0
        assert failed.after() == 0.0

    def test_non_numeric_record_count_does_not_raise(self):
        assert cm.record_crawl_run(_run(records_read="not a number")) is True

    def test_backwards_clock_does_not_produce_negative_duration(self):
        total = _Delta("kbo_crawl_duration_seconds_sum", crawler="backwards")

        cm.record_crawl_run(
            _run(
                crawler="backwards",
                started_at=datetime(2026, 1, 8, 0, 5, 0),
                finished_at=datetime(2026, 1, 8, 0, 0, 0),
            )
        )

        assert total.after() == pytest.approx(0.0)

    def test_run_without_duration_still_refreshes_success(self):
        cm.record_crawl_run(_run(finished_at=None))

        assert _sample("kbo_crawl_last_success_timestamp", crawler="awards") > 0.0

    def test_zero_elapsed_duration_is_observable(self):
        count = _Delta("kbo_crawl_duration_seconds_count", crawler="instant")

        cm.record_crawl_run(
            _run(
                crawler="instant",
                started_at=datetime(2026, 1, 8, 0, 0, 0),
                finished_at=datetime(2026, 1, 8, 0, 0, 0),
            )
        )

        assert count.after() == 1.0

    def test_unparseable_status_is_reported_as_unknown(self):
        delta = _Delta("kbo_crawl_runs_total", crawler="awards", status="unknown")

        assert cm.record_crawl_run(_run(status="weird status")) is True
        assert delta.after() == 1.0


class TestImportIsSideEffectFree:
    """Track 1's lesson: import-time behaviour is what only shows up in a full run."""

    def test_fresh_interpreter_can_import_the_module(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import src.monitoring.crawler_metrics as m; print(m.CRAWL_RUNS_TOTAL._name)",
            ],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert "kbo_crawl_runs" in result.stdout

    def test_import_does_not_pull_in_the_database_layer(self):
        """Metrics must not drag the ORM or the ledger into a metrics import."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys; import src.monitoring.crawler_metrics;"
                " print(','.join(m for m in sys.modules"
                " if m.startswith('src.db') or m.startswith('src.models')))",
            ],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "", f"unexpected imports: {result.stdout.strip()}"

    def test_metrics_are_exported_for_the_contract_test(self):
        exported = set(cm.__all__)

        assert "record_crawl_run" in exported
        assert "CRAWL_RUNS_TOTAL" in exported
        assert "CRAWL_LAST_SUCCESS_TIMESTAMP" in exported


def test_timezone_naive_finish_is_treated_as_utc():
    cm.record_crawl_run(_run(finished_at=datetime(2026, 1, 8, 0, 0, 0)))

    expected = datetime(2026, 1, 8, 0, 0, 0, tzinfo=UTC).timestamp()
    assert _sample("kbo_crawl_last_success_timestamp", crawler="awards") == pytest.approx(expected)


def test_duration_uses_the_difference_not_the_wall_clock():
    start = datetime(2026, 1, 8, 0, 0, 0)
    total = _Delta("kbo_crawl_duration_seconds_sum", crawler="timed")
    cm.record_crawl_run(
        _run(
            crawler="timed",
            started_at=start,
            finished_at=start + timedelta(seconds=45),
        )
    )

    assert total.after() == pytest.approx(45.0)
