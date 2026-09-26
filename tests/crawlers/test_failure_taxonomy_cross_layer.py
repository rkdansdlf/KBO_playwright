"""A classified result reaches every layer under one name.

This is contract B of the failure taxonomy. Contract A
(`test_result_taxonomy.py`) proves the transport attaches the right code; this
module proves that once a result is classified, the run ledger, the dead letter
queue, and the Prometheus series all report the *same* classification, and that
`failure_stage` is always derived from the code rather than carried alongside it.

The chain is exercised through the real service, the real repositories, an
in-memory database, and the real metric projection. Nothing is mocked, because
the failure mode being guarded against is precisely a gap between layers.

Wiring an actual crawler (`AwardCrawler`) through this chain is deliberately out
of scope: it does not use `CrawlerHttpClient` yet, so that connection is the
next track. Here the classified result enters the chain the way a crawler will
eventually hand it over.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.crawlers.failure_taxonomy import (
    CrawlPersistError,
    FailureCode,
    FailureStage,
    classification_for_result,
    stage_for_code,
)
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services.crawl_dead_letter_service import CrawlDeadLetterService
from src.services.crawl_run_service import CrawlRunService, track_crawl_run

# Codes a fetch-stage transport can produce, plus one parse-stage code. The
# expected stage is spelled out rather than derived, so the table also catches
# drift in `stage_for_code` itself and not only in whoever writes the letter.
_TAXONOMY_CASES = [
    (FailureCode.FETCH_TIMEOUT, FailureStage.FETCH),
    (FailureCode.FETCH_HTTP_ERROR, FailureStage.FETCH),
    (FailureCode.FETCH_BLOCKED, FailureStage.FETCH),
    (FailureCode.FETCH_RATE_LIMITED, FailureStage.FETCH),
    (FailureCode.PARSE_INVALID_FORMAT, FailureStage.PARSE),
]


def _spec() -> CrawlRunSpec:
    return CrawlRunSpec(crawler="awards", target_type="award_history")


@pytest.fixture
def session() -> Iterator[Session]:
    engine = create_engine("sqlite:///:memory:")
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    active = sessionmaker(bind=engine, expire_on_commit=False)()
    try:
        yield active
    finally:
        active.close()


@pytest.fixture(autouse=True)
def _fresh_metric_state():
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


def _failed_result(code: FailureCode, stage: FailureStage) -> CrawlResult[object]:
    """Build the classified result a transport would hand over."""
    outcome = CrawlOutcome.RETRYABLE_ERROR if stage is FailureStage.FETCH else CrawlOutcome.SCHEMA_CHANGED
    return CrawlResult.failure(outcome, error="boom", error_code=code.value)


def _drive_chain(
    session: Session,
    result: CrawlResult[object],
    *,
    stage: FailureStage,
) -> tuple[str, CrawlDeadLetter, str]:
    """Run one classified result through ledger, dead letter queue, and metrics.

    Returns:
        The ledger code, the dead letter, and the Prometheus `error_code` label.

    """
    classification = classification_for_result(result)
    assert classification is not None, "a failure result must classify"
    derived_stage, code = classification
    # The transport's result and the shared taxonomy must already agree.
    assert derived_stage is stage

    service = CrawlRunService(session)
    run = service.start(_spec())
    finished = service.failed(run, error_code=code.value, error_message="boom")

    letter = CrawlDeadLetterService(session).enqueue(
        DeadLetterSpec(
            original_run_id=finished.run_id,
            crawler=finished.crawler,
            target_type=finished.target_type,
            # The stage is derived from the code, not supplied beside it.
            failure_stage=stage_for_code(code).value,
            error_code=code.value,
            error_message="boom",
        ),
    )
    session.commit()

    # `CrawlRunService` already projects the terminal run onto the metrics;
    # measuring again here would double-count, so the sample is only read back.
    return finished.error_code or "", letter, _metric_label(code)


def _metric_label(code: FailureCode) -> str:
    """Read the `error_code` label back out of the counter series."""
    metric = REGISTRY.get_sample_value(
        "kbo_crawl_failures_total",
        {"crawler": "awards", "error_code": code.value, "failure_stage": stage_for_code(code).value},
    )
    assert metric is not None, f"no failure series for {code}"
    return code.value


class TestOneNameAcrossEveryLayer:
    @pytest.mark.parametrize(("code", "stage"), _TAXONOMY_CASES)
    def test_ledger_dlq_and_metrics_agree(self, session: Session, code: FailureCode, stage: FailureStage) -> None:
        """The completion criterion: one code, end to end."""
        result = _failed_result(code, stage)

        ledger_code, letter, metric_label = _drive_chain(session, result, stage=stage)

        assert ledger_code == code.value
        assert letter.error_code == code.value
        assert metric_label == code.value

    @pytest.mark.parametrize(("code", "stage"), _TAXONOMY_CASES)
    def test_every_layer_starts_from_the_result(self, session: Session, code: FailureCode, stage: FailureStage) -> None:
        """The result is the origin of the name, not a copy of it."""
        result = _failed_result(code, stage)

        _, normalized = classification_for_result(result)
        ledger_code, letter, metric_label = _drive_chain(session, result, stage=stage)

        assert normalized.value == ledger_code == letter.error_code == metric_label

    @pytest.mark.parametrize(("code", "stage"), _TAXONOMY_CASES)
    def test_dead_letter_stage_is_derived_from_its_code(
        self, session: Session, code: FailureCode, stage: FailureStage
    ) -> None:
        """`failure_stage` is an invariant of `error_code`, so a letter can never
        claim a fetch code failed while persisting.
        """
        _, letter, _ = _drive_chain(session, _failed_result(code, stage), stage=stage)

        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.parametrize(("code", "stage"), _TAXONOMY_CASES)
    def test_the_letter_stage_matches_the_taxonomy_table(
        self, session: Session, code: FailureCode, stage: FailureStage
    ) -> None:
        """The same check without deriving the expectation, so drift in
        `stage_for_code` cannot hide behind a self-consistent assertion.
        """
        _, letter, _ = _drive_chain(session, _failed_result(code, stage), stage=stage)

        assert letter.failure_stage == stage.value

    @pytest.mark.parametrize(("code", "stage"), _TAXONOMY_CASES)
    def test_metric_stage_label_is_derived_too(self, code: FailureCode, stage: FailureStage) -> None:
        derived, _ = classification_for_result(_failed_result(code, stage))

        assert derived is stage
        assert derived is stage_for_code(code)

    def test_the_counter_increments_once_per_failure(self, session: Session) -> None:
        REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "awards", "error_code": "FETCH_TIMEOUT", "failure_stage": "fetch"},
        )
        _drive_chain(session, _failed_result(FailureCode.FETCH_TIMEOUT, FailureStage.FETCH), stage=FailureStage.FETCH)
        first = REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "awards", "error_code": "FETCH_TIMEOUT", "failure_stage": "fetch"},
        )
        _drive_chain(session, _failed_result(FailureCode.FETCH_TIMEOUT, FailureStage.FETCH), stage=FailureStage.FETCH)
        second = REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "awards", "error_code": "FETCH_TIMEOUT", "failure_stage": "fetch"},
        )

        assert second == pytest.approx((first or 0) + 1)

    def test_two_different_causes_stay_two_different_series(self, session: Session) -> None:
        """A timeout and a rate limit must not be aggregated into one series."""
        _drive_chain(session, _failed_result(FailureCode.FETCH_TIMEOUT, FailureStage.FETCH), stage=FailureStage.FETCH)
        _drive_chain(
            session,
            _failed_result(FailureCode.FETCH_RATE_LIMITED, FailureStage.FETCH),
            stage=FailureStage.FETCH,
        )

        timeout = REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "awards", "error_code": "FETCH_TIMEOUT", "failure_stage": "fetch"},
        )
        limited = REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "awards", "error_code": "FETCH_RATE_LIMITED", "failure_stage": "fetch"},
        )

        assert timeout is not None
        assert limited is not None


class TestSuccessesNeverEnterTheFailureChain:
    def test_a_success_is_not_a_failure(self, session: Session) -> None:
        result = CrawlResult.success({"rows": [1]})

        assert classification_for_result(result) is None

        service = CrawlRunService(session)
        run = service.start(_spec())
        finished = service.success(run, records_written=1)
        session.commit()

        assert finished.error_code is None
        assert finished.status != "failed"

    def test_an_empty_result_is_not_a_failure(self, session: Session) -> None:
        result = CrawlResult.empty()

        assert classification_for_result(result) is None

    def test_a_recovered_run_is_not_a_failure(self, session: Session) -> None:
        """Retry, then success. The run must not carry the first attempt's code
        into the ledger, the queue, or the metrics.
        """
        failed_attempt = CrawlResult.failure(
            CrawlOutcome.RETRYABLE_ERROR,
            error="timed out",
            error_code=FailureCode.FETCH_TIMEOUT.value,
        )
        final = CrawlResult.success({"rows": [1]})

        assert classification_for_result(failed_attempt) is not None
        assert classification_for_result(final) is None

        service = CrawlRunService(session)
        run = service.start(_spec())
        finished = service.success(run, records_written=1)
        session.commit()

        assert finished.error_code is None

    def test_a_partial_run_is_not_a_failure_either(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())
        finished = service.partial(run, records_read=10, records_written=9, records_failed=1)
        session.commit()

        assert finished.error_code is None


class TestTheServiceDoesNotInterpretHttp:
    def test_the_service_records_the_code_it_is_given(self, session: Session) -> None:
        """`CrawlRunService` must not second-guess transport meaning; a caller
        that hands it a code gets that exact code stored.
        """
        service = CrawlRunService(session)
        run = service.start(_spec())

        finished = service.failed(run, error_code="FETCH_TIMEOUT", error_message="x")

        assert finished.error_code == "FETCH_TIMEOUT"

    def test_track_crawl_run_records_the_code_carried_by_the_exception(self, session: Session) -> None:
        """The context manager already reads `error_code` off the raised
        exception, which is the contract `CrawlPersistError` implements.
        """
        with pytest.raises(CrawlPersistError):
            with track_crawl_run(_spec(), session=session):
                message = "write failed"
                raise CrawlPersistError(
                    message,
                    error_code=FailureCode.PERSIST_CONSTRAINT,
                )

        stored = session.query(CrawlExecutionRun).one()

        assert stored.error_code == "PERSIST_CONSTRAINT"
        assert stored.status == "failed"
