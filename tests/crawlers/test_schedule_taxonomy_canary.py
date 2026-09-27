"""The schedule canary: a month, and the case that is easy to get wrong.

The schedule feeds nearly every other crawl, so a quiet gap here shows up
everywhere downstream as missing data. That makes the *negative* contract the
one worth pinning, and it is the same shape the roster canary proved for a
quiet day:

    an off-season month must not launch a browser,
    must not appear as a failure metric, and must not enqueue a dead letter.

It also proves the one thing schedule has that roster does not: a month is
assembled day by day, so a month where *some* days fail is incomplete and must
be reported as a failure rather than as a short but authoritative answer.

Everything below runs for real. Only the socket is faked.
"""

from __future__ import annotations

import calendar
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import date

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
from src.crawlers.schedule_crawler import (
    NAVER_SCHEDULE_API_URL,
    SCHEDULE_CRAWLER_NAME,
    SCHEDULE_TARGET_TYPE,
    ScheduleCrawler,
)
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm
from src.services import crawl_replay_dispatcher as dispatcher_module
from src.services.crawl_dead_letter_service import CrawlDeadLetterService
from src.services.crawl_replay_dispatcher import build_default_dispatcher
from src.services.crawl_retry_policy import decide

YEAR = 2026
MONTH = 3
TARGET = f"{YEAR}-{MONTH:02d}"
DAYS_IN_MONTH = calendar.monthrange(YEAR, MONTH)[1]

#: A day response with one game, shaped like the Naver schedule API.
GAMES_PAYLOAD: dict[str, object] = {
    "result": {
        "games": [
            {
                "gameId": "20260315HTHH0",
                "gameDate": "2026-03-15",
                "gameDateTime": "2026-03-15T18:30:00",
                "awayTeamCode": "HT",
                "homeTeamCode": "HH",
                "statusCode": "BEFORE",
                "statusInfo": "경기전",
                "cancel": False,
                "suspended": False,
            },
        ],
    },
}

#: A day response with no games: an ordinary day of an off-season month.
EMPTY_DAY_PAYLOAD: dict[str, object] = {"result": {"games": []}}


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
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


@pytest.fixture(autouse=True)
def _allow_kbo_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.crawlers.schedule_crawler.compliance.is_allowed",
        _stub(True),
    )


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.utils.throttle import throttle

    monkeypatch.setenv("KBO_REQUEST_DELAY", "0")
    monkeypatch.setenv("KBO_REQUEST_JITTER", "0")
    monkeypatch.setattr(throttle, "default_delay", 0.0)
    monkeypatch.setattr(throttle, "jitter", 0.0)
    monkeypatch.setattr(throttle, "_last_request_times", {})


def _stub(value: bool):
    from unittest.mock import AsyncMock

    return AsyncMock(return_value=value)


def _day_of(request: httpx.Request) -> int:
    """Read the requested day out of the query string."""
    return int(request.url.params["date"][-2:])


def _crawler(handler) -> ScheduleCrawler:
    """A real crawler whose Naver transport runs over a mock socket."""
    client = CrawlerHttpClient(
        name=SCHEDULE_CRAWLER_NAME,
        policy=HttpPolicy(
            base_delay_seconds=0.0,
            max_attempts=1,
            max_backoff_seconds=0.0,
            circuit=CircuitPolicy(failure_threshold=999),
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
    return ScheduleCrawler(http_client=client)


def _quiet_month(_: httpx.Request) -> httpx.Response:
    """Every day answers, and no day has a game."""
    return httpx.Response(200, json=EMPTY_DAY_PAYLOAD, headers={"Content-Type": "application/json"})


def _one_game(request: httpx.Request) -> httpx.Response:
    """Only the 15th has a game."""
    payload = GAMES_PAYLOAD if _day_of(request) == 15 else EMPTY_DAY_PAYLOAD
    return httpx.Response(200, json=payload, headers={"Content-Type": "application/json"})


def _failing_day(request: httpx.Request) -> httpx.Response:
    """One day times out; the rest of the month is fine."""
    if _day_of(request) == 15:
        raise httpx.ReadTimeout("slow")
    return httpx.Response(200, json=EMPTY_DAY_PAYLOAD, headers={"Content-Type": "application/json"})


def _run_sample(status: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "kbo_crawl_runs_total",
            {"crawler": SCHEDULE_CRAWLER_NAME, "status": status},
        )
        or 0.0
    )


def _failure_sample(code: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": SCHEDULE_CRAWLER_NAME, "error_code": code, "failure_stage": stage_for_code(code).value},
        )
        or 0.0
    )


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


class TestAnOffSeasonMonthIsData:
    """The contract this track exists to establish."""

    @pytest.mark.asyncio
    async def test_a_quiet_month_never_launches_a_browser(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The bug this crawler had: an empty list fell through to Playwright."""
        crawler = _crawler(_quiet_month)
        browser = _browser_spy(monkeypatch, crawler)

        games = await crawler.crawl_schedule(YEAR, MONTH)

        assert games == []
        browser.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_quiet_month_is_a_successful_run(self, session_factory: sessionmaker) -> None:
        before = _run_sample("success")
        await _crawler(_quiet_month).crawl_schedule(YEAR, MONTH)

        run = _only_run(session_factory)

        assert run.status == "success"
        assert run.error_code is None
        assert run.records_read == 0
        assert _run_sample("success") - before == 1.0

    @pytest.mark.asyncio
    async def test_a_quiet_month_is_never_a_failure_metric(self, session_factory: sessionmaker) -> None:
        before = {
            code: _failure_sample(code.value)
            for code in (FailureCode.PARSE_SELECTOR_MISSING, FailureCode.UNKNOWN, FailureCode.FETCH_TIMEOUT)
        }

        await _crawler(_quiet_month).crawl_schedule(YEAR, MONTH)

        for code, count in before.items():
            assert _failure_sample(code.value) == count, f"{code} was raised for an off-season month"

    @pytest.mark.asyncio
    async def test_a_quiet_month_enqueues_no_dead_letter(self, session_factory: sessionmaker) -> None:
        await _crawler(_quiet_month).crawl_schedule(YEAR, MONTH)

        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_the_empty_result_carries_no_error_code(self) -> None:
        result = await _crawler(_quiet_month)._crawl_naver_month(YEAR, MONTH)

        assert result.outcome is CrawlOutcome.EMPTY
        assert result.error_code is None

    @pytest.mark.asyncio
    async def test_every_day_is_queried(self) -> None:
        """A month assembled day by day must actually ask for every day."""
        seen: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(_day_of(request))
            return httpx.Response(200, json=EMPTY_DAY_PAYLOAD)

        await _crawler(handler)._crawl_naver_month(YEAR, MONTH)

        assert seen == list(range(1, DAYS_IN_MONTH + 1))


class TestAMonthWithGames:
    @pytest.mark.asyncio
    async def test_games_are_returned_and_recorded(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        crawler = _crawler(_one_game)
        browser = _browser_spy(monkeypatch, crawler)

        games = await crawler.crawl_schedule(YEAR, MONTH)

        assert len(games) == 1
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.records_read == 1
        browser.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_target_is_the_month(self, session_factory: sessionmaker) -> None:
        """The replay unit is a month, so the ledger row must name the month."""
        await _crawler(_one_game).crawl_schedule(YEAR, MONTH)

        run = _only_run(session_factory)

        assert run.crawler == SCHEDULE_CRAWLER_NAME
        assert run.target_type == SCHEDULE_TARGET_TYPE
        assert run.target_id == TARGET
        assert run.season == YEAR


class TestAnIncompleteMonthIsAFailure:
    @pytest.mark.asyncio
    async def test_one_failed_day_makes_the_month_incomplete(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reporting a short month as fact is how missing dates get stored."""
        crawler = _crawler(_failing_day)
        # Block the browser so the failure surfaces instead of being recovered.
        monkeypatch.setattr(
            "src.crawlers.schedule_crawler.compliance.is_allowed",
            _stub(False),
        )
        monkeypatch.setattr(crawler, "_crawl_month", _stub_failure("kbo page unavailable"))

        await crawler.crawl_schedule(YEAR, MONTH)
        run = _only_run(session_factory)

        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value

    @pytest.mark.asyncio
    async def test_the_taxonmy_reaches_the_dead_letter_queue(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        crawler = _crawler(_failing_day)
        monkeypatch.setattr(
            "src.crawlers.schedule_crawler.compliance.is_allowed",
            _stub(False),
        )
        monkeypatch.setattr(crawler, "_crawl_month", _stub_failure("kbo page unavailable"))

        await crawler.crawl_schedule(YEAR, MONTH)
        letter = _letters(session_factory)[0]

        assert letter.error_code == FailureCode.FETCH_TIMEOUT.value
        assert letter.failure_stage == stage_for_code(letter.error_code).value
        assert letter.target_id == TARGET

    @pytest.mark.asyncio
    async def test_the_failure_metric_carries_the_same_labels(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        crawler = _crawler(_failing_day)
        monkeypatch.setattr(
            "src.crawlers.schedule_crawler.compliance.is_allowed",
            _stub(False),
        )
        monkeypatch.setattr(crawler, "_crawl_month", _stub_failure("kbo page unavailable"))
        before = _failure_sample(FailureCode.FETCH_TIMEOUT)

        await crawler.crawl_schedule(YEAR, MONTH)

        assert _failure_sample(FailureCode.FETCH_TIMEOUT) > before

    @pytest.mark.asyncio
    async def test_a_rate_limit_is_not_a_generic_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429, text="slow down")

        result = await _crawler(handler)._crawl_naver_month(YEAR, MONTH)

        assert result.error_code == FailureCode.FETCH_RATE_LIMITED.value


class TestTheBrowserFallback:
    @pytest.mark.asyncio
    async def test_a_failed_api_that_the_browser_answers_is_a_success(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)
        monkeypatch.setattr(
            crawler,
            "_crawl_month",
            _async_stub([{"game_id": "from kbo"}]),
        )
        _stub_browser(crawler, monkeypatch)

        games = await crawler.crawl_schedule(YEAR, MONTH)
        run = _only_run(session_factory)

        assert games == [{"game_id": "from kbo"}]
        assert run.status == "success"
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_browser_page_with_no_rows_is_still_a_quiet_month(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The browser rendered, so an empty table is an answer, not an outage."""

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)
        monkeypatch.setattr(crawler, "_crawl_month", _async_stub([]))
        _stub_browser(crawler, monkeypatch)

        games = await crawler.crawl_schedule(YEAR, MONTH)
        run = _only_run(session_factory)

        assert games == []
        assert run.status == "success"
        assert run.error_code is None
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_blocked_fallback_is_reported_not_hidden(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Returning an empty list would make 'not consulted' look like 'no games'.

        The canonical cause stays the primary timeout, because a replay starts
        from the Naver API; the block is preserved in the message.
        """

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)
        monkeypatch.setattr(
            "src.crawlers.schedule_crawler.compliance.is_allowed",
            _stub(False),
        )

        await crawler.crawl_schedule(YEAR, MONTH)
        run = _only_run(session_factory)

        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value
        assert FailureCode.FETCH_BLOCKED.value in (run.error_message or "")
        letter = _letters(session_factory)[0]
        assert letter.failure_stage == FailureStage.FETCH.value
        assert decide(letter.error_code, retry_count=1).retryable is True

    @pytest.mark.asyncio
    async def test_a_block_is_the_cause_when_there_is_no_primary(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A series-filtered crawl never touches Naver, so the block is all there is."""
        crawler = _crawler(_quiet_month)
        monkeypatch.setattr(
            "src.crawlers.schedule_crawler.compliance.is_allowed",
            _stub(False),
        )

        await crawler.crawl_schedule(YEAR, MONTH, "1")
        run = _only_run(session_factory)

        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_BLOCKED.value
        assert decide(run.error_code, retry_count=1).retryable is False


class TestRetryPolicyFollowsTheCode:
    def test_a_timeout_is_retried(self) -> None:
        assert decide(FailureCode.FETCH_TIMEOUT.value, retry_count=1).retryable is True

    @pytest.mark.asyncio
    async def test_a_real_letter_drives_the_policy(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        crawler = _crawler(_failing_day)
        monkeypatch.setattr(
            "src.crawlers.schedule_crawler.compliance.is_allowed",
            _stub(False),
        )
        monkeypatch.setattr(crawler, "_crawl_month", _stub_failure("kbo page unavailable"))
        await crawler.crawl_schedule(YEAR, MONTH)

        letter = _letters(session_factory)[0]
        decision = decide(letter.error_code, retry_count=1)

        assert decision.retryable is True
        assert decision.delay_seconds == 60


class TestReplayByMonth:
    @pytest.mark.asyncio
    async def test_a_replay_targets_the_same_month(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with session_factory() as check:
            run = _seed_failed_run(check, "run-original")
            letter = CrawlDeadLetterService(check).enqueue(_spec(run.run_id, TARGET, FailureCode.FETCH_TIMEOUT))
            check.commit()
            dlq_id = letter.dlq_id

        replay_crawler = _crawler(_one_game)
        monkeypatch.setattr(dispatcher_module, "ScheduleCrawler", lambda: replay_crawler)

        outcome = build_default_dispatcher().replay(_reload(session_factory, dlq_id), replay_run_id="run-replay")

        assert outcome.success is True
        with session_factory() as check:
            replay_run = check.query(CrawlExecutionRun).filter_by(run_id="run-replay").one()
        assert replay_run.target_id == TARGET
        assert replay_run.records_read == 1

    @pytest.mark.asyncio
    async def test_a_replay_still_failing_keeps_the_taxonomy(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with session_factory() as check:
            run = _seed_failed_run(check, "run-original")
            letter = CrawlDeadLetterService(check).enqueue(_spec(run.run_id, TARGET, FailureCode.FETCH_TIMEOUT))
            check.commit()
            dlq_id = letter.dlq_id

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        replay_crawler = _crawler(handler)
        monkeypatch.setattr(
            "src.crawlers.schedule_crawler.compliance.is_allowed",
            _stub(False),
        )
        replay_crawler._crawl_month = _stub_failure("kbo page unavailable")
        monkeypatch.setattr(dispatcher_module, "ScheduleCrawler", lambda: replay_crawler)

        outcome = build_default_dispatcher().replay(_reload(session_factory, dlq_id), replay_run_id="run-replay")

        assert outcome.success is False
        assert outcome.error_code == FailureCode.FETCH_TIMEOUT.value

    def test_an_unparseable_replay_target_does_not_crash(self) -> None:
        from src.services.crawl_replay_dispatcher import _month_of

        assert _month_of(None) == (None, 0)
        assert _month_of("not-a-month") == (None, 0)
        assert _month_of("2026-03") == (2026, 3)
        assert _month_of("2026-03-15") == (2026, 3)


# --- helpers -------------------------------------------------------------


def _async_stub(payload: list[dict]) -> object:
    from unittest.mock import AsyncMock

    return AsyncMock(return_value=payload)


def _stub_failure(message: str):
    """A browser path that cannot be used."""
    from unittest.mock import AsyncMock

    async def _raise(*args: object, **kwargs: object) -> list[dict]:
        raise RuntimeError(message)

    return AsyncMock(side_effect=_raise)


def _browser_spy(monkeypatch: pytest.MonkeyPatch, crawler: ScheduleCrawler):
    """Replace the Playwright path and report whether it was entered."""
    from unittest.mock import MagicMock

    spy = MagicMock()
    monkeypatch.setattr(crawler, "page_context", spy)
    return spy


def _stub_browser(crawler: ScheduleCrawler, monkeypatch: pytest.MonkeyPatch) -> None:
    """Let the browser path run without a real Playwright context."""

    @asynccontextmanager
    async def _fake_page():
        yield object()

    monkeypatch.setattr(crawler, "page_context", _fake_page)


def _seed_failed_run(session: Session, run_id: str) -> CrawlExecutionRun:
    """Create the original failed run through the repository, timestamps included."""
    from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec

    run = CrawlExecutionRepository(session).start_run(
        CrawlRunSpec(
            crawler=SCHEDULE_CRAWLER_NAME,
            target_type=SCHEDULE_TARGET_TYPE,
            run_id=run_id,
        ),
    )
    session.flush()
    return run


def _spec(run_id: str, target: str, code: FailureCode):
    from src.repositories.crawl_dead_letter_repository import DeadLetterSpec

    return DeadLetterSpec(
        original_run_id=run_id,
        crawler=SCHEDULE_CRAWLER_NAME,
        target_type=SCHEDULE_TARGET_TYPE,
        target_id=target,
        failure_stage=stage_for_code(code).value,
        error_code=code.value,
    )


def _reload(session_factory: sessionmaker, dlq_id: str) -> CrawlDeadLetter:
    """Re-read a letter on a fresh session, as the dispatcher does."""
    from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository

    with session_factory() as check:
        letter = CrawlDeadLetterRepository(check).get_by_dlq_id(dlq_id)
        assert letter is not None
        check.expunge(letter)
        return letter
