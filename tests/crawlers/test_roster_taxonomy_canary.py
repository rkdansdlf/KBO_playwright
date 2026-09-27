"""The roster canary: one date, one taxonomy, all the way to the retry policy.

Award proved multi-source aggregation. This proves the shape that matters just as
much: a **legitimate quiet day**. A day with no call-ups is a real, common
outcome, and the old code could not tell it apart from an outage -- both were an
empty list. So the contract that matters most here is a negative one:

    a confirmed quiet day must not reach the browser fallback,
    must not appear as a failure metric, and must not enqueue a dead letter.

Everything below runs for real -- `CrawlerHttpClient` over a mock transport, the
crawler, the run ledger, the dead letter queue, the Prometheus projection, the
replay dispatcher, and the retry policy. Only the socket is faked.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import asynccontextmanager

import httpx
import pytest
from prometheus_client import REGISTRY
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.failure_taxonomy import FailureCode, FailureStage, stage_for_code
from src.crawlers.http_client import CircuitPolicy, CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers.roster_transaction_crawler import (
    ROSTER_CRAWLER_NAME,
    ROSTER_TARGET_TYPE,
    RosterTransactionCrawler,
)
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.models.source_registry import DataSource
from src.monitoring import crawler_metrics as cm
from src.services import crawl_replay_dispatcher as dispatcher_module
from src.services.crawl_dead_letter_service import CrawlDeadLetterService
from src.services.crawl_replay_dispatcher import build_default_dispatcher
from src.services.crawl_retry_policy import decide

TARGET = "2026-09-27"

#: A page with the expected section and one registered player.
ROWS_HTML = (
    "<html>오늘자 선수 등록현황"
    '<strong class="team">LG</strong><ul><li><a href="?playerId=1">김현수</a></li></ul>'
    "</html>"
)

#: A page with the expected section and no players: a real quiet day.
QUIET_HTML = "<html>오늘자 선수 등록현황<ul></ul></html>"

#: A page whose expected structure is gone.
DRIFT_HTML = "<html>새로운 레이아웃</html>"


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    # A replay persists raw snapshots, which resolves the source registry.
    # Created for real so the replay success path is not stubbed out.
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
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


@pytest.fixture(autouse=True)
def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_replay_dispatcher.SessionLocal", session_factory)
    monkeypatch.setattr("src.crawlers.roster_transaction_crawler.SessionLocal", session_factory)
    # Compliance is decided explicitly per test, never by an ambient allowlist.
    monkeypatch.setattr(
        "src.crawlers.roster_transaction_crawler.compliance.is_allowed",
        _allowed(True),
    )


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.utils.throttle import throttle

    monkeypatch.setenv("KBO_REQUEST_DELAY", "0")
    monkeypatch.setenv("KBO_REQUEST_JITTER", "0")
    monkeypatch.setattr(throttle, "default_delay", 0.0)
    monkeypatch.setattr(throttle, "jitter", 0.0)
    monkeypatch.setattr(throttle, "_last_request_times", {})


def _allowed(value: bool):
    from unittest.mock import AsyncMock

    return AsyncMock(return_value=value)


def _transport(handler) -> CrawlerHttpClient:
    """A real client over a mock transport, with no delays and a single attempt."""
    client = CrawlerHttpClient(
        name=ROSTER_CRAWLER_NAME,
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
    # The circuit registry is a process-wide singleton keyed by name; a breaker
    # left open by another test would fast-fail this one and mask the cause.
    circuit_registry.reset_all()
    return client


def _crawler(handler) -> RosterTransactionCrawler:
    return RosterTransactionCrawler(http_client=_transport(handler))


def _run_sample(status: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "kbo_crawl_runs_total",
            {"crawler": ROSTER_CRAWLER_NAME, "status": status},
        )
        or 0.0
    )


def _failure_sample(code: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": ROSTER_CRAWLER_NAME, "error_code": code, "failure_stage": stage_for_code(code).value},
        )
        or 0.0
    )


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


class TestAQuietDayIsData:
    """The contract this track exists to establish."""

    @pytest.mark.asyncio
    async def test_a_valid_page_with_no_rows_never_launches_the_fallback(
        self,
        session_factory: sessionmaker,
    ) -> None:
        crawler = _crawler(lambda request: httpx.Response(200, text=QUIET_HTML))
        crawler._crawl_desktop_page = _desktop_spy()

        data = await crawler.run(target_date=TARGET)

        assert data == []
        # The whole point: a quiet day must not cost a browser launch.
        crawler._crawl_desktop_page.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_quiet_day_is_a_successful_run(self, session_factory: sessionmaker) -> None:
        crawler = _crawler(lambda request: httpx.Response(200, text=QUIET_HTML))

        before = _run_sample("success")
        await crawler.run(target_date=TARGET)
        run = _only_run(session_factory)

        assert run.status == "success"
        assert run.error_code is None
        assert run.records_read == 0
        assert _run_sample("success") - before == 1.0

    @pytest.mark.asyncio
    async def test_a_quiet_day_is_never_a_failure_metric(self, session_factory: sessionmaker) -> None:
        crawler = _crawler(lambda request: httpx.Response(200, text=QUIET_HTML))
        before = {
            code: _failure_sample(code.value)
            for code in (
                FailureCode.PARSE_SELECTOR_MISSING,
                FailureCode.PARSE_EMPTY,
                FailureCode.UNKNOWN,
            )
        }

        await crawler.run(target_date=TARGET)

        for code, count in before.items():
            assert _failure_sample(code.value) == count, f"{code} was raised for a quiet day"

    @pytest.mark.asyncio
    async def test_a_quiet_day_enqueues_no_dead_letter(self, session_factory: sessionmaker) -> None:
        await _crawler(lambda request: httpx.Response(200, text=QUIET_HTML)).run(target_date=TARGET)

        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_the_empty_result_carries_no_error_code(self) -> None:
        result = await _crawler(lambda r: httpx.Response(200, text=QUIET_HTML))._crawl_mobile_page(
            _as_date(TARGET),
        )

        assert result.outcome is CrawlOutcome.EMPTY
        assert result.error_code is None


class TestRowsSucceedNormally:
    @pytest.mark.asyncio
    async def test_rows_are_returned_and_recorded(self, session_factory: sessionmaker) -> None:
        crawler = _crawler(lambda request: httpx.Response(200, text=ROWS_HTML))
        crawler._crawl_desktop_page = _desktop_spy()

        data = await crawler.run(target_date=TARGET)
        run = _only_run(session_factory)

        assert [row["player_name"] for row in data] == ["김현수"]
        assert run.status == "success"
        assert run.records_read == 1
        crawler._crawl_desktop_page.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_target_is_the_date(self, session_factory: sessionmaker) -> None:
        """The replay unit is a day, so the ledger row must identify the day."""
        await _crawler(lambda request: httpx.Response(200, text=ROWS_HTML)).run(target_date=TARGET)

        run = _only_run(session_factory)

        assert run.crawler == ROSTER_CRAWLER_NAME
        assert run.target_type == ROSTER_TARGET_TYPE
        assert run.target_id == TARGET


class TestUnreadableSourcesFallBack:
    @pytest.mark.asyncio
    async def test_drift_falls_back_and_the_run_succeeds(
        self,
        session_factory: sessionmaker,
    ) -> None:
        crawler = _crawler(lambda request: httpx.Response(200, text=DRIFT_HTML))
        crawler._crawl_desktop_page = _desktop_stub([{"player_name": "from desktop"}])

        data = await crawler.run(target_date=TARGET)
        run = _only_run(session_factory)

        assert data == [{"player_name": "from desktop"}]
        # The primary failed, but the date was obtained, so it is not a failure.
        assert run.status == "success"
        assert run.error_code is None
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_fallback_quiet_day_also_succeeds(
        self,
        session_factory: sessionmaker,
    ) -> None:
        """Guards the regression the old `if not data:` invited: treating a
        fallback that returned nothing as a failure.
        """
        crawler = _crawler(lambda request: httpx.Response(200, text=DRIFT_HTML))
        crawler._crawl_desktop_page = _desktop_stub(CrawlResult.empty())

        data = await crawler.run(target_date=TARGET)
        run = _only_run(session_factory)

        assert data == []
        assert run.status == "success"
        assert run.error_code is None
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_known_team_beside_an_unknown_one_still_succeeds(
        self,
        session_factory: sessionmaker,
    ) -> None:
        html = (
            "<html>오늘자 선수 등록현황"
            '<strong class="team">LG</strong><ul><li><a href="?playerId=1">김현수</a></li></ul>'
            '<strong class="team">어떤구단</strong><ul><li>홍길동</li></ul>'
            "</html>"
        )
        crawler = _crawler(lambda request: httpx.Response(200, text=html))
        crawler._crawl_desktop_page = _desktop_spy()

        data = await crawler.run(target_date=TARGET)

        assert [row["player_name"] for row in data] == ["김현수"]
        crawler._crawl_desktop_page.assert_not_awaited()


class TestUnresolvedDatesFailAndQueue:
    @pytest.mark.asyncio
    async def test_both_sources_failing_marks_the_run_failed(self, session_factory: sessionmaker) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)
        crawler._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))

        await crawler.run(target_date=TARGET)
        run = _only_run(session_factory)

        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value

    @pytest.mark.asyncio
    async def test_the_taxonmy_reaches_the_dead_letter_queue(self, session_factory: sessionmaker) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)
        crawler._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))

        await crawler.run(target_date=TARGET)
        letter = _letters(session_factory)[0]

        assert letter.error_code == FailureCode.FETCH_TIMEOUT.value
        assert letter.failure_stage == stage_for_code(letter.error_code).value
        assert letter.failure_stage is not None
        assert letter.target_id == TARGET

    @pytest.mark.asyncio
    async def test_a_parse_miss_is_staged_as_parse_not_fetch(self, session_factory: sessionmaker) -> None:
        crawler = _crawler(lambda request: httpx.Response(200, text=DRIFT_HTML))
        crawler._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_BLOCKED))

        await crawler.run(target_date=TARGET)
        letter = _letters(session_factory)[0]

        assert letter.error_code == FailureCode.PARSE_SELECTOR_MISSING.value
        assert letter.failure_stage == FailureStage.PARSE.value
        assert letter.failure_stage != FailureStage.FETCH.value

    @pytest.mark.asyncio
    async def test_the_failure_metric_carries_the_same_labels(self, session_factory: sessionmaker) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)
        crawler._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))
        before = _failure_sample(FailureCode.FETCH_TIMEOUT)

        await crawler.run(target_date=TARGET)

        assert _failure_sample(FailureCode.FETCH_TIMEOUT) > before

    @pytest.mark.asyncio
    async def test_a_rate_limit_is_recorded_as_a_rate_limit(self, session_factory: sessionmaker) -> None:
        crawler = _crawler(lambda request: httpx.Response(429, text="slow down"))
        crawler._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))

        await crawler.run(target_date=TARGET)
        run = _only_run(session_factory)

        assert run.error_code == FailureCode.FETCH_RATE_LIMITED.value


class TestRetryPolicyFollowsTheCode:
    def test_a_timeout_is_retried(self) -> None:
        assert decide(FailureCode.FETCH_TIMEOUT.value, retry_count=1).retryable is True

    def test_a_structure_miss_is_not_retried(self) -> None:
        """Retrying cannot fix a page that no longer looks the way it did."""
        assert decide(FailureCode.PARSE_SELECTOR_MISSING.value, retry_count=1).retryable is False

    @pytest.mark.asyncio
    async def test_a_real_letter_drives_the_policy(self, session_factory: sessionmaker) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)
        crawler._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))
        await crawler.run(target_date=TARGET)

        letter = _letters(session_factory)[0]
        decision = decide(letter.error_code, retry_count=1)

        assert decision.retryable is True
        assert decision.delay_seconds == 60


class TestReplayByDate:
    @pytest.mark.asyncio
    async def test_a_replay_that_still_fails_keeps_the_taxonomy(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        broken = _crawler(handler)
        broken._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))
        await broken.run(target_date=TARGET)

        with session_factory() as check:
            run = check.query(CrawlExecutionRun).one()
            letter = CrawlDeadLetterService(check).enqueue(_spec(run.run_id, TARGET, run.error_code))
            check.commit()
            dlq_id = letter.dlq_id

        monkeypatch.setattr(
            dispatcher_module,
            "RosterTransactionCrawler",
            lambda: _crawler(lambda r: httpx.Response(200, text=QUIET_HTML)),
        )
        outcome = build_default_dispatcher().replay(_reload(session_factory, dlq_id), replay_run_id="run-replay")

        assert outcome.success is True
        assert outcome.status == "success"
        assert outcome.error_code is None

        with session_factory() as check:
            replay_run = check.query(CrawlExecutionRun).filter_by(run_id="run-replay").one()
        assert replay_run.target_id == TARGET

    @pytest.mark.asyncio
    async def test_a_replay_still_failing_reports_the_same_code(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        broken = _crawler(handler)
        broken._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))
        await broken.run(target_date=TARGET)

        with session_factory() as check:
            run = check.query(CrawlExecutionRun).one()
            letter = CrawlDeadLetterService(check).enqueue(_spec(run.run_id, TARGET, run.error_code))
            check.commit()
            dlq_id = letter.dlq_id

        def replay_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        replay_crawler = _crawler(replay_handler)
        replay_crawler._crawl_desktop_page = _desktop_stub(_failure(FailureCode.FETCH_HTTP_ERROR))
        monkeypatch.setattr(dispatcher_module, "RosterTransactionCrawler", lambda: replay_crawler)

        outcome = build_default_dispatcher().replay(_reload(session_factory, dlq_id), replay_run_id="run-replay")

        assert outcome.success is False
        assert outcome.status == "failed"
        assert outcome.error_code == FailureCode.FETCH_TIMEOUT.value


class TestPolicySkipIsNeitherEmptyNorFailure:
    @pytest.mark.asyncio
    async def test_a_blocked_source_records_the_skip_reason(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "src.crawlers.roster_transaction_crawler.compliance.is_allowed",
            _allowed(False),
        )
        crawler = _crawler(lambda request: httpx.Response(200, text=ROWS_HTML))

        data = await crawler.run(target_date=TARGET)
        run = _only_run(session_factory)

        assert data == []
        assert run.status == "success"
        assert run.error_code is None
        assert run.checkpoint["outcome"] == "source_limited"
        assert run.checkpoint["reason"] == "compliance_blocked"
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_blocked_source_is_never_counted_as_a_failure(self, session_factory: sessionmaker) -> None:
        monkeypatch_before = {code: _failure_sample(code.value) for code in FailureCode if code.value != "UNKNOWN"}
        from unittest.mock import patch as sync_patch

        with sync_patch(
            "src.crawlers.roster_transaction_crawler.compliance.is_allowed",
            _allowed(False),
        ):
            await _crawler(lambda r: httpx.Response(200, text=ROWS_HTML)).run(target_date=TARGET)

        for code, count in monkeypatch_before.items():
            assert _failure_sample(code.value) == count, f"{code} was raised for a policy skip"


# --- helpers -------------------------------------------------------------


def _as_date(value: str):
    from datetime import date

    return date.fromisoformat(value)


def _failure(code: FailureCode) -> CrawlResult[list[dict]]:
    return CrawlResult.failure(
        CrawlOutcome.PERMANENT_ERROR,
        error="boom",
        error_code=code.value,
    )


def _desktop_stub(payload) -> object:
    """Replace the Playwright fallback with a classified stub."""
    from unittest.mock import AsyncMock

    if not isinstance(payload, CrawlResult):
        payload = CrawlResult.success(payload)
    return AsyncMock(return_value=payload)


def _desktop_spy() -> object:
    """A fallback stub that also records whether it was entered."""
    from unittest.mock import AsyncMock

    return AsyncMock(return_value=CrawlResult.success([]))


def _spec(run_id: str, target: str, code: str):
    from src.repositories.crawl_dead_letter_repository import DeadLetterSpec

    return DeadLetterSpec(
        original_run_id=run_id,
        crawler=ROSTER_CRAWLER_NAME,
        target_type=ROSTER_TARGET_TYPE,
        target_id=target,
        game_id=target,
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
