"""The Naver record API and the KBO page, and which of them gets to explain.

Game detail has two sources, and unlike the roster or the schedule the Naver
answer is *not* final: Naver has no record for plenty of games that the KBO
GameCenter page still has. So an empty Naver result is not a success and not a
stop -- it is a reason to ask the other source. Copying the "EMPTY means do not
fall back" rule from the other crawlers would file every one of those games as
missing.

Emptiness also has to be recognised twice. The transport sees an empty HTTP JSON
body, but Naver normally answers a missing record with `{"result": {}}`, which is
a perfectly good response carrying no `recordData`. Only the second check can
tell those apart, and treating a malformed envelope as an empty one would file a
broken response as "no record for this game".

The tests below drive the real crawler over a mock socket and check the outcomes
the run ledger will later record.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.game_detail_outcome import (
    GameDetailSources,
    GameDetailStatus,
    canonical_failure_code,
    resolve_attempt,
)
from src.crawlers.http_client import CircuitPolicy, CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_execution import CrawlExecutionRun

GAME_A = "20250501LGOB0"
GAME_B = "20250502KTSS0"

#: A Naver envelope carrying both sides of the box score.
RECORD_ENVELOPE: dict = {
    "result": {
        "recordData": {
            "gameInfo": {"aCode": "LG", "hCode": "OB", "aName": "LG", "hName": "OB"},
            "scoreBoard": {"rheb": {"away": {"r": "3"}, "home": {"r": "5"}}, "inn": {"away": [1, 2], "home": [0, 5]}},
            "battersBoxscore": {"away": [{"name": "김현수", "hr": 1}], "home": [{"name": "박용택"}]},
            "pitchersBoxscore": {"away": [{"name": "신meh"}], "home": [{"name": "박연준"}]},
        },
    },
}

#: A Naver envelope that carries only one side of the box score.
ONE_SIDED_RECORD: dict = {
    "result": {
        "recordData": {
            "gameInfo": {"aCode": "LG", "hCode": "OB"},
            "scoreBoard": {"rheb": {"away": {"r": "3"}, "home": {"r": "5"}}},
            "battersBoxscore": {"away": [{"name": "김현수", "hr": 1}], "home": []},
            "pitchersBoxscore": {"away": [{"name": "신원종"}], "home": []},
        },
    },
}


#: A complete box score, as the KBO GameCenter page produces.
FULL_DETAIL: dict = {
    "game_id": GAME_A,
    "hitters": {"away": [{}], "home": [{}]},
    "pitchers": {"away": [{}], "home": [{}]},
    "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "OB", "score": 5}},
    "metadata": {"stadium": "잠실"},
}


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch) -> sessionmaker:
    """Give the run ledger a table, so the batch path is exercised as in production."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", factory)
    return factory


@pytest.fixture
def session(ledger: sessionmaker) -> Iterator[object]:
    active = ledger()
    try:
        yield active
    finally:
        active.close()


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.utils.throttle import throttle

    monkeypatch.setenv("KBO_REQUEST_DELAY", "0")
    monkeypatch.setenv("KBO_REQUEST_JITTER", "0")
    monkeypatch.setattr(throttle, "default_delay", 0.0)
    monkeypatch.setattr(throttle, "jitter", 0.0)
    monkeypatch.setattr(throttle, "_last_request_times", {})


def _naver_transport(handler) -> CrawlerHttpClient:
    client = CrawlerHttpClient(
        name="game_detail_naver",
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
    return client


def _json(payload: object, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload, headers={"Content-Type": "application/json"})


def _crawler(
    naver_handler, *, kbo_payload=None, kbo_reason: str | None = None, robots: bool = True
) -> GameDetailCrawler:
    """Build a crawler with a mocked Naver socket and a mocked KBO page."""
    pool = MagicMock(max_pages=2)
    pool.start = AsyncMock()
    pool.acquire = AsyncMock(side_effect=[MagicMock(), MagicMock()])
    pool.release = AsyncMock()
    pool.close = AsyncMock()
    crawler = GameDetailCrawler(resolver=MagicMock(), pool=pool, naver_http=_naver_transport(naver_handler))

    if kbo_reason is not None:
        crawler._kbo_fallback_allowed = AsyncMock(return_value=robots)  # type: ignore[method-assign]
        crawler._last_failure_reason[GAME_A] = kbo_reason

        async def _fail(_page, game_id, _game_date, *, lightweight):
            crawler._last_failure_reason[game_id] = kbo_reason
            return kbo_payload

    else:

        async def _fail(_page, game_id, _game_date, *, lightweight):
            return kbo_payload

    crawler._crawl_single = AsyncMock(side_effect=_fail)  # type: ignore[method-assign]
    return crawler


async def _attempts(crawler: GameDetailCrawler, *games: str, robots: bool = True) -> list:
    targets = [{"game_id": g, "game_date": "20250501"} for g in games]
    with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=robots)):
        return await crawler.crawl_game_attempts(targets, concurrency=2)


class TestNaverOutcomes:
    @pytest.mark.asyncio
    async def test_a_record_is_a_success_and_skips_the_browser(self) -> None:
        crawler = _crawler(lambda r: _json(RECORD_ENVELOPE))

        attempts = await _attempts(crawler, GAME_A)

        assert attempts[0].status is GameDetailStatus.SUCCESS
        assert attempts[0].source == "naver_record_api"
        crawler._crawl_single.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_result_without_record_data_is_an_application_empty(self) -> None:
        """`{"result": {}}` is a healthy response carrying no record."""
        crawler = _crawler(lambda r: _json({"result": {}}))

        result = await crawler._crawl_naver_single(GAME_A, "20250501")

        assert result.outcome is CrawlOutcome.EMPTY
        assert result.error_code is None

    @pytest.mark.asyncio
    async def test_an_entirely_empty_body_is_also_empty(self) -> None:
        crawler = _crawler(lambda r: _json({}))

        result = await crawler._crawl_naver_single(GAME_A, "20250501")

        assert result.outcome is CrawlOutcome.EMPTY

    @pytest.mark.asyncio
    async def test_an_empty_result_triggers_the_browser(self) -> None:
        """The rule that is unique to game detail: Naver EMPTY is not final."""
        crawler = _crawler(lambda r: _json({"result": {}}), kbo_payload=dict(FULL_DETAIL))

        attempts = await _attempts(crawler, GAME_A)

        assert attempts[0].status is GameDetailStatus.SUCCESS
        assert attempts[0].source == "kbo_gamecenter"
        crawler._crawl_single.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_malformed_envelope_is_a_parse_failure(self) -> None:
        """`result` as a list is not a Naver envelope, and filing it as an empty
        record would hide a broken response behind a missing game.
        """
        crawler = _crawler(lambda r: _json({"result": []}))

        result = await crawler._crawl_naver_single(GAME_A, "20250501")

        assert result.error_code == "PARSE_INVALID_FORMAT"

    @pytest.mark.asyncio
    async def test_a_malformed_envelope_still_asks_the_browser(self) -> None:
        crawler = _crawler(lambda r: _json({"result": []}), kbo_payload=dict(FULL_DETAIL))

        attempts = await _attempts(crawler, GAME_A)

        assert attempts[0].status is GameDetailStatus.SUCCESS
        assert crawler._crawl_single.await_count == 1

    @pytest.mark.asyncio
    async def test_the_primary_never_writes_a_failure_reason(self) -> None:
        """A mutable side channel from the primary is how a fallback overwrites
        the cause that started it. Attribution happens once, at the end.
        """
        crawler = _crawler(lambda r: _json({"result": {}}))

        await crawler._crawl_naver_single(GAME_A, "20250501")

        assert crawler.get_last_failure_reason(GAME_A) is None

    @pytest.mark.asyncio
    async def test_a_timeout_is_classified_by_the_transport(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler)

        result = await crawler._crawl_naver_single(GAME_A, "20250501")

        assert result.error_code == "FETCH_TIMEOUT"

    @pytest.mark.asyncio
    async def test_a_one_sided_record_is_a_partial_not_a_success(self) -> None:
        """A Naver record that carries only one side's box score is genuinely
        incomplete. The collection service already stores that with
        `allow_partial=True` and marks it `detail_status="partial"`, so recording
        it as a success here would contradict what is written to the database.
        """
        crawler = _crawler(lambda r: _json(ONE_SIDED_RECORD))

        result = await crawler._crawl_naver_single(GAME_A, "20250501")

        assert result.ok
        attempts = [resolve_attempt(GAME_A, result.data, sources=GameDetailSources(primary=result), lightweight=False)]

        assert attempts[0].status is GameDetailStatus.PARTIAL
        assert attempts[0].needs_refetch is True


class TestCanonicalCause:
    """Only a primary that actually has a cause gets to claim it."""

    def _attempt(self, primary, fallback_reason, payload=None):
        return resolve_attempt(
            GAME_A,
            payload,
            sources=GameDetailSources(primary=primary, fallback_reason=fallback_reason),
            lightweight=False,
        )

    def test_a_primary_success_claims_nothing(self) -> None:
        code = canonical_failure_code(CrawlResult.success({"a": 1}), "timeout")

        assert code == "FETCH_TIMEOUT"  # the fallback's own code, if it ever failed

    def test_an_empty_primary_defers_to_the_fallback(self) -> None:
        """EMPTY is not a failure, so it has no claim on the cause."""
        code = canonical_failure_code(CrawlResult.empty(), "navigation_error")

        assert code == "FETCH_HTTP_ERROR"

    def test_a_meaningful_primary_code_wins(self) -> None:
        primary = CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="x", error_code="FETCH_TIMEOUT")

        assert canonical_failure_code(primary, "navigation_error") == "FETCH_TIMEOUT"

    def test_an_unknown_primary_defers_to_a_known_fallback(self) -> None:
        primary = CrawlResult.failure(CrawlOutcome.PERMANENT_ERROR, error="x", error_code="UNKNOWN")

        assert canonical_failure_code(primary, "navigation_error") == "FETCH_HTTP_ERROR"

    def test_a_parse_failure_from_the_primary_wins(self) -> None:
        primary = CrawlResult.failure(CrawlOutcome.SCHEMA_CHANGED, error="x", error_code="PARSE_INVALID_FORMAT")

        assert canonical_failure_code(primary, "timeout") == "PARSE_INVALID_FORMAT"


class TestEndToEndOutcomes:
    @pytest.mark.asyncio
    async def test_a_timeout_recovered_by_the_browser_records_no_failure(
        self,
        ledger: sessionmaker,
    ) -> None:
        """A recovered game must not be reported as broken downstream."""

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler, kbo_payload=dict(FULL_DETAIL))

        attempts = await _attempts(crawler, GAME_A)

        assert attempts[0].status is GameDetailStatus.SUCCESS
        assert attempts[0].error_code is None
        assert crawler.get_last_failure_reason(GAME_A) is None

    @pytest.mark.asyncio
    async def test_a_timeout_with_a_failed_fallback_reports_the_timeout(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _crawler(handler, kbo_payload=None, kbo_reason="navigation_error")

        attempts = await _attempts(crawler, GAME_A)

        assert attempts[0].status is GameDetailStatus.FAILED
        assert attempts[0].error_code == "FETCH_TIMEOUT"
        assert crawler.get_last_failure_reason(GAME_A) == "timeout"

    @pytest.mark.asyncio
    async def test_an_empty_naver_with_a_failed_fallback_reports_the_fallback(
        self,
    ) -> None:
        """Naver having no record is not the cause of the failure."""
        crawler = _crawler(lambda r: _json({"result": {}}), kbo_payload=None, kbo_reason="navigation_error")

        attempts = await _attempts(crawler, GAME_A)

        assert attempts[0].status is GameDetailStatus.FAILED
        assert attempts[0].error_code == "FETCH_HTTP_ERROR"
        assert crawler.get_last_failure_reason(GAME_A) == "navigation_error"

    @pytest.mark.asyncio
    async def test_a_blocked_browser_after_an_empty_naver_is_attributed_to_the_block(
        self,
    ) -> None:
        """We needed the KBO page and could not have it. That is a real cause."""
        crawler = _crawler(lambda r: _json({"result": {}}), robots=False)

        attempts = await _attempts(crawler, GAME_A, robots=False)

        assert attempts[0].status is GameDetailStatus.FAILED
        assert crawler.get_last_failure_reason(GAME_A) == "kbo_robots_blocked"

    @pytest.mark.asyncio
    async def test_mixed_games_keep_independent_outcomes(self, ledger: sessionmaker) -> None:
        """Concurrency must not let one game's failure land on another."""

        def handler(request: httpx.Request) -> httpx.Response:
            return _json(RECORD_ENVELOPE)

        crawler = _crawler(handler, kbo_payload={**FULL_DETAIL, "game_id": GAME_B})
        targets = [{"game_id": GAME_A, "game_date": "20250501"}, {"game_id": GAME_B, "game_date": "20250501"}]
        with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=True)):
            attempts = await crawler.crawl_game_attempts(targets, concurrency=2)

        assert [a.game_id for a in attempts] == [GAME_A, GAME_B]
        assert all(a.status is GameDetailStatus.SUCCESS for a in attempts)
        # A game answered by Naver must not be attributed to the browser.
        assert attempts[0].source == "naver_record_api"
