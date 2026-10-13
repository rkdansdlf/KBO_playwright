"""Unit tests for the Wikipedia draft crawler (offline fixtures)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.crawlers.draft_wiki_crawler import (
    WIKI_API_URL,
    DraftWikiCrawler,
    _is_first_pick_table,
    _is_round_table,
    _round_order_even_reversed,
    parse_draft_cell,
)
from src.crawlers.http_client import CrawlerHttpClient
from src.crawlers.result import CrawlOutcome, CrawlResult

HEADERS = ["순위", "한화", "삼성"]

#: A 2차-style wikitable wide enough for the round-table detector.
SECOND_ROUND_TABLE = """
<table class="wikitable">
  <tr><th>순위</th><th>한화</th><th>삼성</th><th>롯데</th><th>두산</th><th>NC</th></tr>
  <tr><td>1</td><td>-</td><td>김선수(부산고,포수)(계약금:1억원)</td><td>-</td><td>-</td><td>-</td></tr>
</table>
"""


def _page(*tables: str) -> CrawlResult[Any]:
    """A successful `action=parse` result carrying the given tables."""
    return CrawlResult.success(
        {"parse": {"text": {"*": "<html><body>" + "".join(tables) + "</body></html>"}}},
        http_status=200,
    )


def _missing() -> CrawlResult[Any]:
    """How Wikipedia answers a page that does not exist: HTTP 200 plus `error`."""
    return CrawlResult.success({"error": {"info": "missing"}}, http_status=200)


def _stub_fetch(crawler: DraftWikiCrawler, *results: CrawlResult[Any]) -> AsyncMock:
    """Route the crawler's transport to canned results, never the network.

    Stubbing `crawler._http` rather than injecting a client is what keeps the
    URL validator's DNS resolution out of the test. Assigning the attribute on
    the instance is what `fetch_season_tables` actually calls.
    """
    fetch = AsyncMock(side_effect=list(results))
    crawler._http.fetch_json = fetch  # type: ignore[method-assign]
    return fetch


def test_parse_draft_cell_full() -> None:
    """Cell with school, position, and fee parses into fields."""
    parsed = parse_draft_cell("황준서(장충고,투수)(계약금:3억 5천만원)")
    assert parsed is not None
    assert parsed["player_name"] == "황준서"
    assert parsed["school"] == "장충고"
    assert parsed["position"] == "투수"
    assert parsed["sign_fee"] == "3억 5천만원"


def test_parse_draft_cell_empty() -> None:
    """Placeholder cells return None."""
    assert parse_draft_cell("-") is None
    assert parse_draft_cell("") is None


def test_round_order_snake() -> None:
    """Even rounds reverse the column order."""
    assert _round_order_even_reversed(1, 3) == [0, 1, 2]
    assert _round_order_even_reversed(2, 3) == [2, 1, 0]


def test_table_detection() -> None:
    """Round tables and 1차 tables are distinguished."""
    wide = ["순위", "한화", "삼성", "롯데", "두산", "NC"]
    assert _is_round_table(wide, [["1", "a", "b", "c", "d", "e"]])
    assert not _is_round_table(["구단", "학교"], [["x"]])
    assert _is_first_pick_table(["구단", "학교", "포지션"])
    assert not _is_first_pick_table(wide)


def test_parse_season_records_resolves_teams() -> None:
    """Records carry season/type/round/pick and resolved team codes."""
    crawler = DraftWikiCrawler()
    rows = [["1", "김선수(서울고,투수)(계약금:1억원)", "이선수(부산고,포수)"]]
    records = crawler.parse_season_records(
        2024, HEADERS, rows, team_resolver=lambda name, season: {"한화": "HH", "삼성": "SS"}.get(name)
    )
    assert len(records) == 2
    assert records[0]["team_code"] == "HH"
    assert records[0]["round_num"] == 1
    assert records[0]["pick_seq"] == 1
    assert records[1]["pick_seq"] == 2
    assert records[0]["draft_type"] == "2차"


def test_parse_first_pick_records() -> None:
    """1차 rows map team/school/position/name/fee columns."""
    crawler = DraftWikiCrawler()
    rows = [["삼성 라이온즈", "경북고", "투수", "박선수", "3억원"]]
    records = crawler.parse_first_pick_records(2024, rows)
    assert len(records) == 1
    assert records[0]["draft_type"] == "1차"
    assert records[0]["player_name"] == "박선수"
    assert records[0]["school"] == "경북고"


class TestTheCrawlerFetchesThroughTheGovernedClient:
    """The transport contract `scripts/lint_crawler_client_bypass.py` enforces."""

    def test_the_transport_is_a_crawler_http_client(self) -> None:
        """A raw httpx client would opt out of throttling, retry, and the circuit.

        `scripts/lint_crawler_client_bypass.py` fails the build when a crawler
        constructs `httpx.AsyncClient` itself, so this asserts the same
        condition from the other direction: what the crawler holds.
        """
        assert isinstance(DraftWikiCrawler()._http, CrawlerHttpClient)

    def test_the_request_delay_reaches_the_rate_limiter(self) -> None:
        """A caller-supplied delay stays authoritative instead of being dropped.

        `CrawlerHttpClient` defaults its adaptive delay to 0.5s. Silently
        serving crawlers half the requested spacing would turn a deliberate
        throttle into a milder one nobody asked for.
        """
        crawler = DraftWikiCrawler(request_delay=2.5)
        assert crawler._http.policy.base_delay_seconds == 2.5

    def test_an_injected_client_is_used(self) -> None:
        """An injected transport replaces the default rather than being ignored."""
        client = CrawlerHttpClient(name="test")
        assert DraftWikiCrawler(http_client=client)._http is client


class TestSeasonPageResolution:
    """Which Wikipedia title answered, and what that means for the outcome."""

    @pytest.mark.asyncio
    async def test_a_missing_first_title_falls_back_to_the_second(self) -> None:
        """The second title is tried when the first page does not exist."""
        crawler = DraftWikiCrawler()
        fetch = _stub_fetch(crawler, _missing(), _page(SECOND_ROUND_TABLE))

        result = await crawler.fetch_season_tables(2024)

        assert result.ok
        assert fetch.await_count == 2
        assert [call.kwargs["params"]["page"] for call in fetch.await_args_list] == [
            "2024년 KBO 리그 신인 드래프트",
            "2024년 한국프로야구 신인 드래프트",
        ]

    @pytest.mark.asyncio
    async def test_a_present_first_title_stops_the_search(self) -> None:
        """The fallback title is not requested once a page answered."""
        crawler = DraftWikiCrawler()
        fetch = _stub_fetch(crawler, _page(SECOND_ROUND_TABLE))

        result = await crawler.fetch_season_tables(2024)

        assert result.ok
        assert fetch.await_count == 1

    @pytest.mark.asyncio
    async def test_both_titles_missing_is_empty_not_a_failure(self) -> None:
        """A season with no Wikipedia page is an empty answer, not an exception.

        Wikipedia answers a missing page with HTTP 200 and an `error` key. The
        old code raised on any non-2xx, which made an empty season and a broken
        request look identical to the caller: both surfaced as one failure with
        no classification to tell them apart.
        """
        crawler = DraftWikiCrawler()
        _stub_fetch(crawler, _missing(), _missing())

        result = await crawler.fetch_season_tables(2024)

        assert result.outcome is CrawlOutcome.EMPTY
        assert not result.ok

    @pytest.mark.asyncio
    async def test_a_transport_failure_keeps_its_classification(self) -> None:
        """A failed request is returned as-is, carrying the code the ledger records.

        This is the distinction the empty case above depends on: `EMPTY` means
        "no data this season", while a retryable fault must stay retryable so
        the caller can tell it from a season that genuinely has no draft.
        """
        crawler = DraftWikiCrawler()
        failure = CrawlResult.failure(
            CrawlOutcome.RETRYABLE_ERROR,
            error="Connection reset by peer",
            error_code="FETCH_HTTP_ERROR",
            retry_after=12.0,
            url=WIKI_API_URL,
        )
        _stub_fetch(crawler, failure)

        result = await crawler.fetch_season_tables(2024)

        # Identity, not equality: rebuilding the failure would compare equal on
        # the fields that happen to be copied while silently dropping the rest.
        assert result is failure
        assert result.outcome is CrawlOutcome.RETRYABLE_ERROR
        assert result.error_code == "FETCH_HTTP_ERROR"
        assert result.retry_after == 12.0

    @pytest.mark.asyncio
    async def test_a_transport_failure_does_not_try_the_second_title(self) -> None:
        """A broken request is not retried against a different title.

        The client already retried the same request internally; asking the
        second title would report two faults for one outage.
        """
        crawler = DraftWikiCrawler()
        fetch = _stub_fetch(
            crawler,
            CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="timeout", error_code="FETCH_HTTP_ERROR"),
        )

        await crawler.fetch_season_tables(2024)

        assert fetch.await_count == 1

    @pytest.mark.asyncio
    async def test_the_wiki_user_agent_is_sent(self) -> None:
        """The bot user agent is still required to read the parse API."""
        crawler = DraftWikiCrawler()
        fetch = _stub_fetch(crawler, _page(SECOND_ROUND_TABLE))

        await crawler.fetch_season_tables(2024)

        assert fetch.await_args_list[0].kwargs["headers"]["User-Agent"].startswith("KBOPlaywrightBot/")

    @pytest.mark.asyncio
    async def test_the_wiki_api_is_the_only_target(self) -> None:
        """Requests still go to the parse endpoint, not a derived URL."""
        crawler = DraftWikiCrawler()
        fetch = _stub_fetch(crawler, _page(SECOND_ROUND_TABLE))

        await crawler.fetch_season_tables(2024)

        assert fetch.await_args_list[0].args[0] == WIKI_API_URL


class TestCrawlSeasonOutcomes:
    """`crawl_season` is what the CLI consumes, so its contract is the CLI's."""

    @pytest.mark.asyncio
    async def test_a_page_with_a_table_produces_records(self) -> None:
        """The happy path still returns parsed draft records."""
        crawler = DraftWikiCrawler()
        _stub_fetch(crawler, _page(SECOND_ROUND_TABLE))

        result = await crawler.crawl_season(2024)

        assert result.ok
        assert [record["player_name"] for record in result.data] == ["김선수"]

    @pytest.mark.asyncio
    async def test_an_unexpected_body_shape_is_reported_as_schema_drift(self) -> None:
        """A 200 that decodes to a non-object is drift, not an unclassified crash.

        `fetch_json` promises a decoded body, not the shape of one. Indexing a
        list or a string raises `TypeError`, which would escape the crawler as
        an exception and lose the classification the ledger records.
        """
        crawler = DraftWikiCrawler()
        _stub_fetch(crawler, CrawlResult.success(["unexpected"], http_status=200))

        result = await crawler.fetch_season_tables(2024)

        assert result.outcome is CrawlOutcome.SCHEMA_CHANGED
        assert result.error_code == "PARSE_INVALID_FORMAT"

    @pytest.mark.asyncio
    async def test_a_missing_season_yields_empty(self) -> None:
        """A season without a page returns empty, so the caller saves nothing."""
        crawler = DraftWikiCrawler()
        _stub_fetch(crawler, _missing(), _missing())

        result = await crawler.crawl_season(2024)

        assert result.outcome is CrawlOutcome.EMPTY

    @pytest.mark.asyncio
    async def test_a_page_without_a_draft_table_yields_empty(self) -> None:
        """An existing page carrying no draft table is also empty."""
        crawler = DraftWikiCrawler()
        _stub_fetch(crawler, _page('<table class="wikitable"><tr><td>범례</td></tr></table>'))

        result = await crawler.crawl_season(2024)

        assert result.outcome is CrawlOutcome.EMPTY

    @pytest.mark.asyncio
    async def test_a_transport_failure_propagates_unclassified_as_itself(self) -> None:
        """`crawl_season` forwards the failure instead of masking it as empty.

        Returning an empty list here would be the quiet version of the bug: the
        CLI would log "0 records" and exit 0 for an outage, and the scheduler
        would see a successful run.
        """
        crawler = DraftWikiCrawler()
        _stub_fetch(
            crawler,
            CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="boom", error_code="FETCH_HTTP_ERROR"),
        )

        result = await crawler.crawl_season(2024)

        assert result.outcome is CrawlOutcome.RETRYABLE_ERROR
        assert result.error_code == "FETCH_HTTP_ERROR"


class TestTheSingularAccessor:
    """`fetch_season_table` unwraps the 2차 table and must not invent data."""

    @pytest.mark.asyncio
    async def test_it_returns_the_second_round_table(self) -> None:
        """The 2차 headers and rows come back without the 1차 rows."""
        crawler = DraftWikiCrawler()
        _stub_fetch(crawler, _page(SECOND_ROUND_TABLE))

        result = await crawler.fetch_season_table(2024)

        assert result.ok
        headers, rows = result.data
        assert headers[0] == "순위"
        assert rows[0][0] == "1"

    @pytest.mark.asyncio
    async def test_it_reports_empty_when_there_is_no_table(self) -> None:
        """No 2차 table is empty, even when the page itself existed."""
        crawler = DraftWikiCrawler()
        _stub_fetch(crawler, _page('<table class="wikitable"><tr><td>범례</td></tr></table>'))

        result = await crawler.fetch_season_table(2024)

        assert result.outcome is CrawlOutcome.EMPTY
