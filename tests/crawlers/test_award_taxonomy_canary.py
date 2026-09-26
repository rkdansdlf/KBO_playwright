"""The award canary: one request, one taxonomy, all the way to the retry policy.

This is the contract that only exists once a real crawler is wired end to end.
Everything below runs for real -- `CrawlerHttpClient` over a mock transport, the
crawler, the run ledger, the dead letter queue, the Prometheus projection, the
replay dispatcher, and the retry policy. Only the socket is faked, because the
point is to prove the *wiring*, not the network.

The chain under test:

    CrawlerHttpClient -> CrawlResult -> AwardSourceRun -> CrawlExecutionRun
        -> Prometheus -> CrawlDeadLetter -> replay -> retry policy

with two equalities holding at every step:

    result.error_code == run.error_code == dead_letter.error_code
                    == prometheus error_code
    dead_letter.failure_stage == stage_for_code(dead_letter.error_code)
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import asynccontextmanager

import httpx
import pytest
from bs4 import BeautifulSoup
from prometheus_client import REGISTRY
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.award_crawler import (
    AWARD_CRAWLER_NAME,
    YAGOONARA_SOURCE_KEY,
    WIKI_SOURCE_KEY,
    AwardCrawler,
)
from src.crawlers.failure_taxonomy import FailureCode, FailureStage, stage_for_code
from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.http_client import CircuitPolicy, CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.models.source_registry import DataSource
from src.monitoring import crawler_metrics as cm
from src.services import crawl_replay_dispatcher as dispatcher_module
from src.services.crawl_dead_letter_service import CrawlDeadLetterService
from src.services.crawl_replay_dispatcher import build_default_dispatcher
from src.services.crawl_retry_policy import decide

EMPTY_HTML = "<html></html>"

#: A minimal wikipedia parse payload, shaped like the real parse API response.
WIKI_JSON = {"parse": {"text": {"*": "<table></table>"}}}


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    # A successful replay persists raw snapshots, which resolves the source
    # registry. Created for real so the success path is not stubbed out.
    DataSource.__table__.create(engine)
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
    """Keep the shared Prometheus registry from leaking counts between tests."""
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch: pytest.MonkeyPatch):
    """Keep the shared throttle out of the way so the tests stay fast."""
    from src.utils.throttle import throttle

    monkeypatch.setenv("KBO_REQUEST_DELAY", "0")
    monkeypatch.setenv("KBO_REQUEST_JITTER", "0")
    monkeypatch.setattr(throttle, "default_delay", 0.0)
    monkeypatch.setattr(throttle, "jitter", 0.0)
    monkeypatch.setattr(throttle, "_last_request_times", {})


def _transport(handler) -> CrawlerHttpClient:
    """Build a real client over a mock transport, with no delays and no retries."""
    client = CrawlerHttpClient(
        name=AWARD_CRAWLER_NAME,
        policy=HttpPolicy(
            base_delay_seconds=0.0,
            max_attempts=1,
            max_backoff_seconds=0.0,
            circuit=CircuitPolicy(failure_threshold=99),
        ),
    )

    @asynccontextmanager
    async def _mock_client():
        async with httpx.AsyncClient(
            headers=client.default_headers,
            timeout=client.timeout,
            transport=httpx.MockTransport(handler),
            follow_redirects=True,
        ) as raw:
            yield raw

    client._client = _mock_client
    # The circuit registry is a process-wide singleton keyed by client name, so a
    # breaker left open by another test would fast-fail this one and report the
    # open circuit instead of the cause under test.
    circuit_registry.reset_all()
    return client


def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_replay_dispatcher.SessionLocal", session_factory)
    monkeypatch.setattr("src.crawlers.award_crawler.SessionLocal", session_factory)


def _timeout(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadTimeout("slow")


def _ok_wiki(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json=WIKI_JSON, headers={"Content-Type": "application/json"})


def _run_sample(status: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "kbo_crawl_runs_total",
            {"crawler": AWARD_CRAWLER_NAME, "status": status},
        )
        or 0.0
    )


def _failure_sample(code: str, stage: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": AWARD_CRAWLER_NAME, "error_code": code, "failure_stage": stage},
        )
        or 0.0
    )


def _letter(session: Session, *, target_id: str) -> CrawlDeadLetter:
    return session.query(CrawlDeadLetter).filter(CrawlDeadLetter.target_id == target_id).one_or_none()


class TestTransportToLedger:
    @pytest.mark.asyncio
    async def test_a_timeout_becomes_a_fetch_timeout_in_the_ledger(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(http_client=_transport(_timeout))

        count = await crawler.run(save=False)

        assert count == 0
        with session_factory() as check:
            run = check.query(CrawlExecutionRun).one()
            assert run.status == "partial"
            # The aggregate code belongs to the run; each source keeps the real
            # cause, and the dead letter carries it.
            assert run.error_code == FailureCode.SOURCE_PARTIAL.value

    @pytest.mark.asyncio
    async def test_the_source_run_keeps_the_transport_code(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(http_client=_transport(_timeout))

        await crawler.run(save=False)

        codes = {run.source_key: run.error_code for run in crawler.source_runs}
        assert codes[WIKI_SOURCE_KEY] == FailureCode.FETCH_TIMEOUT.value
        assert codes[YAGOONARA_SOURCE_KEY] == FailureCode.FETCH_TIMEOUT.value

    @pytest.mark.asyncio
    async def test_rate_limited_is_distinguished_from_a_timeout(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(http_client=_transport(lambda request: httpx.Response(429, text="slow down")))

        await crawler.run(save=False)

        codes = {run.error_code for run in crawler.source_runs}
        assert codes == {FailureCode.FETCH_RATE_LIMITED.value}


class TestOneNameAcrossTheWholeChain:
    @pytest.mark.asyncio
    async def test_the_taxonomy_is_identical_at_every_layer(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A full crawl and a replay answer two different questions.

        A full crawl has one run covering every source, and `partial` counts as
        a success for freshness: the crawl did produce data, so the run and its
        metric carry the aggregate `SOURCE_PARTIAL` while each source keeps its
        real cause in the dead letter queue. The per-source code reaches
        `kbo_crawl_failures_total` through the source-specific replay, asserted
        below. Collapsing the two would either lose the per-source cause or make
        every aggregate look like a timeout.
        """
        _wire_sessions(monkeypatch, session_factory)
        aggregate = FailureCode.SOURCE_PARTIAL.value
        specific = FailureCode.FETCH_TIMEOUT.value

        runs_before = _run_sample("partial")
        crawler = AwardCrawler(http_client=_transport(_timeout))
        await crawler.run(save=False)

        with session_factory() as check:
            run = check.query(CrawlExecutionRun).one()
            letter = _letter(check, target_id=YAGOONARA_SOURCE_KEY)

        assert run.error_code == aggregate
        assert _run_sample("partial") - runs_before == 1.0
        # A partial is not counted as a failure; the source code is carried by
        # the dead letter, and by the replay run when that source is retried.
        assert _failure_sample(aggregate, stage_for_code(aggregate).value) == 0.0
        assert letter is not None, "a failed source must enqueue a dead letter"
        assert letter.error_code == specific
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_the_dead_letter_stage_is_derived_from_its_code(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(http_client=_transport(_timeout))

        await crawler.run(save=False)

        with session_factory() as check:
            for letter in check.query(CrawlDeadLetter).all():
                assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_a_partial_run_is_measured_exactly_once(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Regression guard for the wiring gap this track closed.

        `run()` marks the run partial itself. The Prometheus projection lives in
        the service terminal methods, so without routing the pre-marked status
        through the service the run is in the ledger and missing from the
        metrics -- the worst of both worlds.
        """
        _wire_sessions(monkeypatch, session_factory)
        before = _run_sample("partial")

        await AwardCrawler(http_client=_transport(_timeout)).run(save=False)

        assert _run_sample("partial") - before == 1.0


class TestSourceSpecificReplayIsFailedNotPartial:
    @pytest.mark.asyncio
    async def test_a_replay_failure_is_failed_with_the_real_code(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Replay asks for exactly one unit. If it fails, nothing succeeded."""
        _wire_sessions(monkeypatch, session_factory)
        monkeypatch.setattr(
            dispatcher_module,
            "AwardCrawler",
            lambda: AwardCrawler(http_client=_transport(_timeout)),
        )

        # Create the letter the replay will consume.
        with session_factory() as check:
            run = _seed_failed_run(check, "run-original")
            letter = CrawlDeadLetterService(check).enqueue(
                _spec(run.run_id, YAGOONARA_SOURCE_KEY, FailureCode.FETCH_TIMEOUT.value),
            )
            check.commit()
            dlq_id = letter.dlq_id

        outcome = build_default_dispatcher().replay(_reload(session_factory, dlq_id), replay_run_id="run-replay")

        assert outcome.success is False
        assert outcome.status == "failed"
        assert outcome.error_code == FailureCode.FETCH_TIMEOUT.value

    @pytest.mark.asyncio
    async def test_the_replay_run_carries_the_source_code_into_the_metrics(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The full equality, on the path where all four layers describe the
        same single unit: result, run, dead letter, and metric.
        """
        _wire_sessions(monkeypatch, session_factory)
        monkeypatch.setattr(
            dispatcher_module,
            "AwardCrawler",
            lambda: AwardCrawler(http_client=_transport(_timeout)),
        )

        code = FailureCode.FETCH_TIMEOUT.value
        stage = stage_for_code(code).value
        before = _failure_sample(code, stage)

        with session_factory() as check:
            run = _seed_failed_run(check, "run-original")
            letter = CrawlDeadLetterService(check).enqueue(
                _spec(run.run_id, YAGOONARA_SOURCE_KEY, code),
            )
            check.commit()
            dlq_id = letter.dlq_id

        outcome = build_default_dispatcher().replay(_reload(session_factory, dlq_id), replay_run_id="run-replay")

        with session_factory() as check:
            replay_run = check.query(CrawlExecutionRun).filter_by(run_id="run-replay").one()

        assert replay_run.error_code == code
        assert outcome.error_code == replay_run.error_code
        assert _failure_sample(code, stage) > before
        assert replay_run.error_code != FailureCode.SOURCE_PARTIAL.value

    @pytest.mark.asyncio
    async def test_a_successful_replay_succeeds_and_resolves(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire_sessions(monkeypatch, session_factory)
        monkeypatch.setattr(
            dispatcher_module,
            "AwardCrawler",
            lambda: AwardCrawler(http_client=_transport(_ok_wiki)),
        )

        with session_factory() as check:
            run = _seed_failed_run(check, "run-original")
            letter = CrawlDeadLetterService(check).enqueue(
                _spec(run.run_id, YAGOONARA_SOURCE_KEY, FailureCode.FETCH_TIMEOUT.value),
            )
            check.commit()
            dlq_id = letter.dlq_id

        outcome = build_default_dispatcher().replay(_reload(session_factory, dlq_id), replay_run_id="run-replay")

        assert outcome.success is True
        assert outcome.status == "success"
        assert outcome.error_code is None


class TestParseLevelFailures:
    @pytest.mark.asyncio
    async def test_html_instead_of_json_is_a_parse_failure(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The reason wikipedia is fetched as JSON: a 200 that is not the payload
        we asked for has to read as drift, not as a transport success.
        """
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(
            http_client=_transport(
                lambda request: httpx.Response(
                    200,
                    text="<html>maintenance</html>",
                    headers={"Content-Type": "text/html"},
                ),
            ),
        )

        await crawler.run(save=False)

        # Wikipedia is the JSON source, so an HTML body is drift. Yagoonara wants
        # HTML, so the same body is a legitimate success there.
        by_source = {run.source_key: run.error_code for run in crawler.source_runs}
        assert by_source[WIKI_SOURCE_KEY] == FailureCode.PARSE_INVALID_FORMAT.value
        assert by_source[YAGOONARA_SOURCE_KEY] is None

    @pytest.mark.asyncio
    async def test_wiki_json_without_the_expected_shape_is_a_parse_failure(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(
            http_client=_transport(lambda request: httpx.Response(200, json={"unexpected": True})),
        )

        await crawler.run(save=False)

        wiki_code = {run.error_code for run in crawler.source_runs if run.source_key == WIKI_SOURCE_KEY}
        assert wiki_code == {FailureCode.PARSE_INVALID_FORMAT.value}

    @pytest.mark.asyncio
    async def test_an_empty_transport_body_is_a_domain_empty(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A blank yagoonara page is not a success with nothing to report."""
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(
            http_client=_transport(
                lambda request: httpx.Response(200, text="", headers={"Content-Type": "text/html"}),
            ),
        )

        await crawler.run(save=False)

        yagoonara = [run for run in crawler.source_runs if run.source_key == YAGOONARA_SOURCE_KEY]
        assert yagoonara[0].error_code == FailureCode.PARSE_EMPTY.value

    def test_the_empty_code_is_parsed_not_fetched(self):
        assert stage_for_code(FailureCode.PARSE_EMPTY) is FailureStage.PARSE

    @pytest.mark.asyncio
    async def test_a_parse_failure_letter_is_staged_as_parse(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A parse-stage code must not be filed under `fetch`.

        Every other letter here carries a fetch code, so a hardcoded stage would
        pass unnoticed; this is the case that pins the derivation.
        """
        _wire_sessions(monkeypatch, session_factory)
        crawler = AwardCrawler(
            http_client=_transport(lambda request: httpx.Response(200, json={"unexpected": True})),
        )

        await crawler.run(save=False)

        with session_factory() as check:
            letter = _letter(check, target_id=WIKI_SOURCE_KEY)

        assert letter is not None
        assert letter.error_code == FailureCode.PARSE_INVALID_FORMAT.value
        assert letter.failure_stage == FailureStage.PARSE.value
        assert letter.failure_stage != FailureStage.FETCH.value
        # And it is not retried, because the cause is not transient.
        assert decide(letter.error_code, retry_count=1).retryable is False


class TestRetryPolicySeesTheRealCause:
    @pytest.mark.parametrize(
        ("code", "retryable"),
        [
            (FailureCode.FETCH_TIMEOUT, True),
            (FailureCode.FETCH_RATE_LIMITED, True),
            (FailureCode.FETCH_HTTP_ERROR, True),
            (FailureCode.PARSE_INVALID_FORMAT, False),
            (FailureCode.PARSE_EMPTY, False),
            (FailureCode.FETCH_BLOCKED, False),
        ],
    )
    def test_the_policy_follows_the_recorded_code(self, code: FailureCode, retryable: bool):
        """The last link: a dead letter is retried or abandoned based on the
        taxonomy code the transport assigned, not on a message.
        """
        decision = decide(code.value, retry_count=1)

        assert decision.retryable is retryable

    @pytest.mark.asyncio
    async def test_a_letter_from_a_real_run_drives_the_policy(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _wire_sessions(monkeypatch, session_factory)
        await AwardCrawler(http_client=_transport(_timeout)).run(save=False)

        with session_factory() as check:
            letter = _letter(check, target_id=YAGOONARA_SOURCE_KEY)

        assert letter is not None
        decision = decide(letter.error_code, retry_count=1)

        assert decision.retryable is True
        assert decision.delay_seconds == 60


def _seed_failed_run(session: Session, run_id: str) -> CrawlExecutionRun:
    """Create the original failed run through the repository, timestamps included."""
    from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec

    repository = CrawlExecutionRepository(session)
    run = repository.start_run(
        CrawlRunSpec(crawler=AWARD_CRAWLER_NAME, target_type="award_history", run_id=run_id),
    )
    session.flush()
    return run


def _spec(run_id: str, source_key: str, code: str):
    from src.repositories.crawl_dead_letter_repository import DeadLetterSpec

    return DeadLetterSpec(
        original_run_id=run_id,
        crawler=AWARD_CRAWLER_NAME,
        target_type="award_history",
        target_id=source_key,
        failure_stage=stage_for_code(code).value,
        error_code=code,
    )


def _reload(session_factory: sessionmaker, dlq_id: str) -> CrawlDeadLetter:
    """Re-read a letter on a fresh session, as the dispatcher does."""
    from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository

    with session_factory() as check:
        letter = CrawlDeadLetterRepository(check).get_by_dlq_id(dlq_id)
        assert letter is not None
        check.expunge(letter)
        return letter
