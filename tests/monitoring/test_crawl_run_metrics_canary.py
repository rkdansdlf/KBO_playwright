"""End-to-end proof that a real crawl run moves the Prometheus series.

The unit tests on `crawler_metrics` check the projection in isolation. This
module drives the same path a crawler takes -- `track_crawl_run` reaching a
terminal transition through the real service and repository -- and asserts the
metrics move. That is the completion criterion for this track: one actual crawl
run recorded, the same failure taxonomy reflected in a metric, and no gap
between what the ledger says and what Prometheus can see.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.crawlers.failure_taxonomy import FailureCode, stage_for_code
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services.crawl_run_service import CrawlRunService, track_crawl_run


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:")
    CrawlExecutionRun.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def session(session_factory: sessionmaker) -> Iterator[Session]:
    active = session_factory()
    try:
        yield active
    finally:
        active.close()


@pytest.fixture(autouse=True)
def _fresh_metric_state():
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


def _spec(**overrides: object) -> CrawlRunSpec:
    data: dict[str, object] = {"crawler": "awards", "target_type": "award_history"}
    data.update(overrides)
    return CrawlRunSpec(**data)  # type: ignore[arg-type]


def _sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


class TestSuccessPath:
    def test_a_real_successful_run_increments_the_run_counter(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())

        before = _sample("kbo_crawl_runs_total", crawler="awards", status="success")
        service.success(run, records_read=10, records_written=8)
        after = _sample("kbo_crawl_runs_total", crawler="awards", status="success")

        assert after - before == 1.0

    def test_record_counters_come_from_the_service_arguments(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())

        read_before = _sample("kbo_crawl_records_read_total", crawler="awards")
        written_before = _sample("kbo_crawl_records_written_total", crawler="awards")
        service.success(run, records_read=120, records_written=115, records_failed=5)
        session.commit()

        assert _sample("kbo_crawl_records_read_total", crawler="awards") - read_before == 120.0
        assert _sample("kbo_crawl_records_written_total", crawler="awards") - written_before == 115.0
        assert _sample("kbo_crawl_records_failed_total", crawler="awards") >= 5.0
        # The gauge reflects the latest run, which is what the write-drop rule reads.
        assert _sample("kbo_crawl_records_written_last", crawler="awards") == 115.0

    def test_duration_is_observed_from_the_persisted_timestamps(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())

        count_before = _sample("kbo_crawl_duration_seconds_count", crawler="awards")
        service.success(run)
        session.commit()
        count_after = _sample("kbo_crawl_duration_seconds_count", crawler="awards")

        assert count_after - count_before == 1.0

    def test_freshness_gauge_is_populated(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())

        service.success(run)
        session.commit()

        assert _sample("kbo_crawl_last_success_timestamp", crawler="awards") > 0.0


class TestPartialPath:
    def test_partial_run_is_recorded_and_counts_as_fresh(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())

        before = _sample("kbo_crawl_runs_total", crawler="awards", status="partial")
        service.partial(run, records_read=50, records_written=20, records_failed=30)
        session.commit()
        after = _sample("kbo_crawl_runs_total", crawler="awards", status="partial")

        assert after - before == 1.0
        # Partial still produced records, so freshness must not be reset.
        assert _sample("kbo_crawl_last_success_timestamp", crawler="awards") > 0.0


class TestFailurePathUsesTheSameTaxonomy:
    @pytest.mark.parametrize(
        "error_code",
        [
            FailureCode.FETCH_RATE_LIMITED,
            FailureCode.FETCH_TIMEOUT,
            FailureCode.PARSE_SELECTOR_MISSING,
            FailureCode.PERSIST_CONSTRAINT,
        ],
    )
    def test_failure_label_matches_the_failure_taxonomy(
        self,
        session: Session,
        error_code: FailureCode,
    ) -> None:
        """The metric must not invent a classification the DLQ does not use."""
        service = CrawlRunService(session)
        run = service.start(_spec())
        expected_stage = str(stage_for_code(error_code))

        before = _sample(
            "kbo_crawl_failures_total",
            crawler="awards",
            error_code=str(error_code),
            failure_stage=expected_stage,
        )
        service.failed(run, error_code=str(error_code), error_message="boom")
        session.commit()
        after = _sample(
            "kbo_crawl_failures_total",
            crawler="awards",
            error_code=str(error_code),
            failure_stage=expected_stage,
        )

        assert after - before == 1.0

    def test_selector_drift_reaches_the_parse_stage(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())

        service.failed(run, error_code=str(FailureCode.PARSE_SELECTOR_MISSING), error_message="no rows")
        session.commit()

        assert (
            _sample(
                "kbo_crawl_failures_total",
                crawler="awards",
                error_code="PARSE_SELECTOR_MISSING",
                failure_stage="parse",
            )
            >= 1.0
        )

    def test_failure_does_not_claim_a_success(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec(crawler="never_ok"))

        service.failed(run, error_code=str(FailureCode.FETCH_TIMEOUT), error_message="timeout")
        session.commit()

        # A crawler that has never succeeded must expose 0, not a timestamp, so
        # the freshness rule can match the series at all.
        assert _sample("kbo_crawl_last_success_timestamp", crawler="never_ok") == 0.0


class TestRetryThenSuccess:
    def test_a_failed_attempt_followed_by_success_reports_both(self, session: Session) -> None:
        service = CrawlRunService(session)
        first = service.start(_spec(attempt=1))
        service.failed(first, error_code=str(FailureCode.FETCH_TIMEOUT), error_message="timeout")
        session.commit()

        second = service.start(_spec(attempt=2))
        service.success(second, records_read=9, records_written=9)
        session.commit()

        # The failure is retained for the burst rule, and the success is recorded.
        assert (
            _sample(
                "kbo_crawl_failures_total",
                crawler="awards",
                error_code="FETCH_TIMEOUT",
                failure_stage="fetch",
            )
            >= 1.0
        )
        assert _sample("kbo_crawl_runs_total", crawler="awards", status="success") >= 1.0
        assert _sample("kbo_crawl_last_success_timestamp", crawler="awards") > 0.0

    def test_exhausted_retries_end_as_failures_not_successes(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec(attempt=5, crawler="exhausted"))

        service.failed(run, error_code=str(FailureCode.FETCH_RATE_LIMITED), error_message="rate limited")
        session.commit()

        assert _sample("kbo_crawl_last_success_timestamp", crawler="exhausted") == 0.0
        assert (
            _sample(
                "kbo_crawl_failures_total",
                crawler="exhausted",
                error_code="FETCH_RATE_LIMITED",
                failure_stage="fetch",
            )
            >= 1.0
        )


class TestTrackCrawlRunContextManager:
    def test_successful_block_emits_a_success(self, session: Session) -> None:
        before = _sample("kbo_crawl_runs_total", crawler="ctx_ok", status="success")

        with track_crawl_run(_spec(crawler="ctx_ok"), session=session) as run:
            run.records_read = 4
            run.records_written = 4

        after = _sample("kbo_crawl_runs_total", crawler="ctx_ok", status="success")
        assert after - before == 1.0
        assert _sample("kbo_crawl_records_written_last", crawler="ctx_ok") == 4.0

    def test_raising_block_emits_a_failure_and_reraises(self, session: Session) -> None:
        before = _sample("kbo_crawl_runs_total", crawler="ctx_fail", status="failed")

        with pytest.raises(RuntimeError, match="boom"), track_crawl_run(_spec(crawler="ctx_fail"), session=session):
            raise RuntimeError("boom")

        after = _sample("kbo_crawl_runs_total", crawler="ctx_fail", status="failed")
        assert after - before == 1.0

    def test_write_drop_is_observable_through_the_context_manager(self, session: Session) -> None:
        """The sequence the write-drop rule detects, produced for real."""
        with track_crawl_run(_spec(crawler="degrading"), session=session) as run:
            run.records_written = 100
        assert _sample("kbo_crawl_records_written_last", crawler="degrading") == 100.0

        with track_crawl_run(_spec(crawler="degrading"), session=session) as run:
            run.records_written = 0
        assert _sample("kbo_crawl_records_written_last", crawler="degrading") == 0.0


class TestMeasurementNeverBreaksACrawl:
    def test_a_metric_failure_does_not_fail_the_run(self, session: Session, monkeypatch) -> None:
        """Emission is best-effort: the ledger row is the source of truth."""
        service = CrawlRunService(session)
        run = service.start(_spec(crawler="resilient"))

        def _boom(_run: object) -> bool:
            raise RuntimeError("prometheus exploded")

        monkeypatch.setattr("src.services.crawl_run_service.record_crawl_run", _boom)

        result = service.success(run, records_read=1, records_written=1)
        session.commit()

        assert result.status == "success"
        assert result.records_written == 1
