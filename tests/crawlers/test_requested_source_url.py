"""A run names the sources it asked for, never the ones it only planned.

The ledger's ``source_url`` is read by ``source_inventory`` as an observation
against the recorded robots policy, so a URL in it is a claim that a request was
made. That claim cannot be made when a run opens, for two crawlers:

* game detail asks the Naver record endpoint first and reaches the KBO page only
  when Naver has no record *and* the policy allows the fallback. A run whose
  fallback was refused asked Naver and nothing else.
* relay fetches the relay endpoint of the id derived from the KBO game id, and
  asks the schedule lookup -- then a second relay endpoint -- only when the first
  fetch came back empty.

Recording a planned-but-unfetched URL would report a compliant run as a blocked
one, and ``source_inventory`` has no way to tell the difference. So both crawlers
keep a per-game record of what they actually requested, and the collection
service reads it when it closes the run.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.game_detail_crawler import NAVER_RECORD_API_URL, GameDetailCrawler
from src.crawlers.http_client import CircuitPolicy, CrawlerHttpClient, HttpPolicy
from src.crawlers.relay_crawler import RelayCrawler, _InningFetch
from src.crawlers.relay_outcome import InningStop
from src.repositories.crawl_execution_repository import (
    MAX_SOURCE_URLS,
    MULTI_SOURCE_SEPARATOR,
    join_source_urls,
)

GAME = "20250501LGOB0"
#: Naver addresses a game by the KBO id plus the season, not by the KBO id alone.
DIRECT_NAVER_ID = RelayCrawler._map_to_naver_id(GAME)
RESOLVED_NAVER_ID = "20250501LGOB03037"  # differs from the derived id, as a corrected lookup would
NAVER_URL = NAVER_RECORD_API_URL.format(naver_id=DIRECT_NAVER_ID)


def _json(payload: object, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload, headers={"Content-Type": "application/json"})


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
    circuit_registry.reset_all()
    return client


def _detail_crawler(naver_handler) -> GameDetailCrawler:
    pool = MagicMock(max_pages=2)
    pool.start = AsyncMock()
    pool.acquire = AsyncMock(side_effect=[MagicMock(), MagicMock()])
    pool.release = AsyncMock()
    pool.close = AsyncMock()
    return GameDetailCrawler(resolver=MagicMock(), pool=pool, naver_http=_naver_transport(naver_handler))


class TestTheJoinRule:
    """The one place the joined value is built, shared by every sweep."""

    def test_every_page_a_sweep_reads_is_named(self) -> None:
        assert join_source_urls(["https://a.example/1", "https://b.example/2"]) == (
            f"https://a.example/1{MULTI_SOURCE_SEPARATOR}https://b.example/2"
        )

    def test_a_page_asked_twice_is_named_once(self) -> None:
        """Seat maps both Jamsil teams to one page; the sweep still reads it once."""
        assert join_source_urls(["https://a.example/1", "https://a.example/1"]) == "https://a.example/1"

    def test_nothing_requested_is_none_rather_than_empty(self) -> None:
        """An empty string would read as a URL that failed every policy check."""
        assert join_source_urls([]) is None
        assert join_source_urls([""]) is None

    def test_a_single_page_is_not_decorated(self) -> None:
        assert join_source_urls(["https://a.example/1"]) == "https://a.example/1"


class TestTheDetailAttempt:
    """Which of the two sources one detail attempt names."""

    @pytest.mark.asyncio
    async def test_the_record_endpoint_is_named_before_the_answer_arrives(self) -> None:
        """A refused request is the one a reader most needs named."""

        def handler(_request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        crawler = _detail_crawler(handler)

        await crawler._crawl_naver_single(GAME, "20250501")

        assert crawler.get_requested_urls(GAME) == (NAVER_URL,)

    @pytest.mark.asyncio
    async def test_the_kbo_page_is_named_when_the_fallback_is_reached(self) -> None:
        crawler = _detail_crawler(lambda _request: _json({"result": {}}))
        crawler._navigate_section = AsyncMock(return_value=(False, "navigation_error", None))

        await crawler._crawl_single(MagicMock(), GAME, "20250501", lightweight=True)

        assert crawler.get_requested_urls(GAME) == (crawler._section_url(GAME, "20250501", "REVIEW"),)

    @pytest.mark.asyncio
    async def test_a_fallback_the_policy_refuses_is_never_recorded(self) -> None:
        """The rule this whole record exists for: asked is not the same as planned."""
        crawler = _detail_crawler(lambda _request: _json({"result": {}}))

        with patch("src.crawlers.game_detail_crawler.compliance.is_allowed", new=AsyncMock(return_value=False)):
            await crawler.crawl_game_attempts([{"game_id": GAME, "game_date": "20250501"}], concurrency=1)

        assert crawler.get_requested_urls(GAME) == (NAVER_URL,)

    @pytest.mark.asyncio
    async def test_a_new_attempt_does_not_inherit_the_previous_sources(self) -> None:
        """A game that used to fall back must not keep reporting that page."""
        crawler = _detail_crawler(lambda _request: _json({"result": {}}))
        crawler._note_requested_url(GAME, crawler._section_url(GAME, "20250501", "REVIEW"))

        await crawler._crawl_naver_single(GAME, "20250501")

        assert crawler.get_requested_urls(GAME) == (NAVER_URL,)

    def test_the_record_is_capped_so_the_ledger_column_cannot_overflow(self) -> None:
        crawler = _detail_crawler(lambda _request: _json({}))

        for index in range(MAX_SOURCE_URLS + 5):
            crawler._note_requested_url(GAME, f"https://a.example/{index}")

        assert len(crawler.get_requested_urls(GAME)) == MAX_SOURCE_URLS


def _relay_crawler() -> RelayCrawler:
    return RelayCrawler(request_delay=0.0)


class TestTheRelayAttempt:
    """The lookup and the second endpoint exist only on the retry path."""

    @pytest.mark.asyncio
    async def test_a_direct_id_that_answers_names_one_endpoint(self) -> None:
        crawler = _relay_crawler()
        crawler._fetch_text_relays = AsyncMock(
            return_value=_InningFetch(relays=[{"seq": 1}], stop=InningStop.COMPLETED, innings_fetched=9),
        )

        await crawler._fetch_with_resolution(GAME, DIRECT_NAVER_ID, None, None)

        assert crawler.get_requested_urls(GAME) == (crawler.api_base_url.format(game_id=DIRECT_NAVER_ID),)

    @pytest.mark.asyncio
    async def test_the_lookup_and_the_resolved_endpoint_are_named_when_they_are_used(self) -> None:
        crawler = _relay_crawler()
        crawler._fetch_text_relays = AsyncMock(
            side_effect=[
                _InningFetch(relays=[], stop=InningStop.EMPTY_INNING, innings_fetched=0),
                _InningFetch(relays=[{"seq": 1}], stop=InningStop.COMPLETED, innings_fetched=9),
            ],
        )
        crawler._resolve_naver_game_id = AsyncMock(return_value=RESOLVED_NAVER_ID)

        await crawler._fetch_with_resolution(GAME, DIRECT_NAVER_ID, None, None)

        assert crawler.get_requested_urls(GAME) == (
            crawler.api_base_url.format(game_id=DIRECT_NAVER_ID),
            crawler.api_base_url.format(game_id=RESOLVED_NAVER_ID),
        )

    @pytest.mark.asyncio
    async def test_the_schedule_lookup_is_named_once_however_many_dates_it_tries(self) -> None:
        """Fifteen spellings of one endpoint would fill the column and say nothing."""
        crawler = _relay_crawler()
        crawler._match_schedule_game = MagicMock(return_value={"gameId": RESOLVED_NAVER_ID})
        crawler._request_json = AsyncMock(return_value=({"result": {"games": [{"gameId": RESOLVED_NAVER_ID}]}}, None))

        resolved = await crawler._resolve_naver_game_id(GAME)

        assert resolved == RESOLVED_NAVER_ID
        assert crawler.get_requested_urls(GAME) == (crawler.schedule_api_base_url,)

    @pytest.mark.asyncio
    async def test_a_new_attempt_starts_with_no_sources(self) -> None:
        crawler = _relay_crawler()
        crawler._fetch_with_resolution = AsyncMock(
            return_value=_InningFetch(relays=[], stop=InningStop.EMPTY_INNING, innings_fetched=0),
        )
        crawler._note_requested_url(GAME, crawler.api_base_url.format(game_id=DIRECT_NAVER_ID))

        await crawler.crawl_game_relay(GAME)

        assert crawler.get_requested_urls(GAME) == ()
