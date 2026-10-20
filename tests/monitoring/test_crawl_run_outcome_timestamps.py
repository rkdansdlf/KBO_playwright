"""The four outcome timestamps must stay distinguishable.

`KboCrawlerNoRecentSuccess` thresholds on the *full*-success timestamp and
`KboCrawlerRecentFailure` compares the failure and run timestamps. Both rules are
wrong in a way no alert test can see if the projection stops separating the four
readings -- a `partial` run advancing the clean-completion gauge would make the
freshness rule permanently quiet for exactly the crawler it exists for, and a
success not clearing the failure reading would keep the failure rule up after the
retry worked.

The unit under test is `record_crawl_run`, called with the shape the service
passes: a terminal run row.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from prometheus_client import REGISTRY

from src.models.crawl_execution import (
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_SUCCESS,
)
from src.monitoring import crawler_metrics as cm

EARLIER = datetime(2026, 10, 17, 6, 45, tzinfo=UTC)
NOW = datetime(2026, 10, 19, 6, 45, tzinfo=UTC)


def _run(crawler: str, status: str, *, finished_at: datetime | None = NOW, error_code: str | None = None):
    return SimpleNamespace(
        crawler=crawler,
        status=status,
        finished_at=finished_at,
        started_at=EARLIER,
        checkpoint=None,
        records_read=3,
        records_written=2,
        records_failed=1,
        error_code=error_code,
    )


def _value(name: str, crawler: str) -> float | None:
    return REGISTRY.get_sample_value(name, {"crawler": crawler})


class TestASingleRun:
    def test_a_success_sets_the_run_and_the_clean_completion(self) -> None:
        cm.record_crawl_run(_run("cadence_success", RUN_STATUS_SUCCESS))

        assert _value("kbo_crawl_last_run_timestamp", "cadence_success") == NOW.timestamp()
        assert _value("kbo_crawl_last_full_success_timestamp", "cadence_success") == NOW.timestamp()
        assert _value("kbo_crawl_last_success_timestamp", "cadence_success") == NOW.timestamp()
        assert _value("kbo_crawl_last_partial_success_timestamp", "cadence_success") == 0.0
        assert _value("kbo_crawl_last_failure_timestamp", "cadence_success") == 0.0

    def test_a_partial_run_does_not_advance_the_clean_completion(self) -> None:
        """Pin the property the freshness rule depends on.

        `partial` is a success for the ledger and must stay one -- reclassifying
        it as a failure would report a working crawler as an outage. What it must
        not do is satisfy a rule whose question is "did a run finish clean".
        """
        cm.record_crawl_run(_run("cadence_partial", RUN_STATUS_PARTIAL))

        assert _value("kbo_crawl_last_run_timestamp", "cadence_partial") == NOW.timestamp()
        assert _value("kbo_crawl_last_success_timestamp", "cadence_partial") == NOW.timestamp()
        assert _value("kbo_crawl_last_partial_success_timestamp", "cadence_partial") == NOW.timestamp()
        assert _value("kbo_crawl_last_full_success_timestamp", "cadence_partial") == 0.0
        assert _value("kbo_crawl_last_failure_timestamp", "cadence_partial") == 0.0

    def test_a_failure_records_the_run_and_the_failure_but_no_success(self) -> None:
        cm.record_crawl_run(_run("cadence_failed", RUN_STATUS_FAILED, error_code="FETCH_HTTP_ERROR"))

        assert _value("kbo_crawl_last_run_timestamp", "cadence_failed") == NOW.timestamp()
        assert _value("kbo_crawl_last_failure_timestamp", "cadence_failed") == NOW.timestamp()
        assert _value("kbo_crawl_last_success_timestamp", "cadence_failed") == 0.0
        assert _value("kbo_crawl_last_full_success_timestamp", "cadence_failed") == 0.0
        assert _value("kbo_crawl_last_partial_success_timestamp", "cadence_failed") == 0.0


class TestASequence:
    def test_a_later_partial_run_leaves_the_clean_completion_where_it_was(self) -> None:
        cm.record_crawl_run(_run("cadence_sequence", RUN_STATUS_SUCCESS, finished_at=EARLIER))
        cm.record_crawl_run(_run("cadence_sequence", RUN_STATUS_PARTIAL, finished_at=NOW))

        assert _value("kbo_crawl_last_run_timestamp", "cadence_sequence") == NOW.timestamp()
        assert _value("kbo_crawl_last_full_success_timestamp", "cadence_sequence") == EARLIER.timestamp()
        assert _value("kbo_crawl_last_partial_success_timestamp", "cadence_sequence") == NOW.timestamp()

    def test_a_success_clears_the_failure_reading(self) -> None:
        """`last_failure >= last_run` is how the failure rule clears.

        The gauge is not reset to zero -- it is history, and zeroing it would lose
        the answer to "when did this last fail". The comparison is what makes a
        later success win, so a success must carry a newer run timestamp.
        """
        cm.record_crawl_run(_run("cadence_recovered", RUN_STATUS_FAILED, finished_at=EARLIER))
        cm.record_crawl_run(_run("cadence_recovered", RUN_STATUS_SUCCESS, finished_at=NOW))

        failure_at = _value("kbo_crawl_last_failure_timestamp", "cadence_recovered")
        run_at = _value("kbo_crawl_last_run_timestamp", "cadence_recovered")

        assert failure_at == EARLIER.timestamp()
        assert run_at == NOW.timestamp()
        assert failure_at < run_at

    def test_a_failure_after_a_partial_run_keeps_the_partial_reading(self) -> None:
        cm.record_crawl_run(_run("cadence_then_failed", RUN_STATUS_PARTIAL, finished_at=EARLIER))
        cm.record_crawl_run(_run("cadence_then_failed", RUN_STATUS_FAILED, finished_at=NOW))

        assert _value("kbo_crawl_last_partial_success_timestamp", "cadence_then_failed") == EARLIER.timestamp()
        assert _value("kbo_crawl_last_failure_timestamp", "cadence_then_failed") == NOW.timestamp()
        assert _value("kbo_crawl_last_full_success_timestamp", "cadence_then_failed") == 0.0


class TestTheReadingsDescribeOneMoment:
    def test_a_run_without_a_finish_time_still_agrees_with_itself(self) -> None:
        """`_epoch_seconds` falls back to "now", so it must be called once.

        Reading the clock per gauge would make the run, success and full-success
        timestamps differ by microseconds -- enough for `last_failure >=
        last_run` to flip on a tie, and enough to make two pipelines reading the
        same run disagree.
        """
        cm.record_crawl_run(_run("cadence_no_finish", RUN_STATUS_SUCCESS, finished_at=None))

        readings = {
            _value("kbo_crawl_last_run_timestamp", "cadence_no_finish"),
            _value("kbo_crawl_last_success_timestamp", "cadence_no_finish"),
            _value("kbo_crawl_last_full_success_timestamp", "cadence_no_finish"),
        }

        assert len(readings) == 1
        assert readings.pop() > 0.0
