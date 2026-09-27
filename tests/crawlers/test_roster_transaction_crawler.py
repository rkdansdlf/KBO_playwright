from datetime import date
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.http_client import CrawlerHttpClient, HttpPolicy
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers.roster_transaction_crawler import ROSTER_CRAWLER_NAME, RosterTransactionCrawler
from src.models.crawl_execution import CrawlExecutionRun


@pytest.fixture(autouse=True)
def allow_kbo_source(monkeypatch):
    monkeypatch.setattr("src.crawlers.roster_transaction_crawler.compliance.is_allowed", AsyncMock(return_value=True))


class FakeResponseContext:
    def __init__(self, *, raises_timeout=False):
        self.raises_timeout = raises_timeout

    async def __aenter__(self):
        if self.raises_timeout:
            raise TimeoutError("response wait timed out")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch) -> sessionmaker:
    """Give the run ledger a real, isolated table.

    `run()` records every attempt, so tests that exercise it need somewhere to
    write. An in-memory database keeps that out of the developer's database.
    """
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", factory)
    return factory


class TestMapTeamName:
    def setup_method(self):
        self.crawler = RosterTransactionCrawler()

    def test_known_team_codes(self):
        assert self.crawler._map_team_name("LG") == "LG"
        assert self.crawler._map_team_name("한화") == "HH"
        assert self.crawler._map_team_name("삼성") == "SS"
        assert self.crawler._map_team_name("두산") == "OB"
        assert self.crawler._map_team_name("롯데") == "LT"

    def test_unknown_returns_none(self):
        assert self.crawler._map_team_name("없음") is None


class TestDedupeTransactions:
    def setup_method(self):
        self.crawler = RosterTransactionCrawler()

    def test_deduplicates_by_dedupe_key(self):
        data = [
            {"dedupe_key": "a", "value": 1},
            {"dedupe_key": "a", "value": 2},
            {"dedupe_key": "b", "value": 3},
        ]
        result = self.crawler._dedupe_transactions(data)
        assert len(result) == 2

    def test_no_dedupe_key_preserved(self):
        data = [
            {"value": 1},
            {"value": 2},
        ]
        result = self.crawler._dedupe_transactions(data)
        assert len(result) == 2

    def test_empty_input(self):
        assert self.crawler._dedupe_transactions([]) == []


class TestRun:
    @pytest.mark.asyncio
    async def test_run_uses_mobile_results_and_saves(self, ledger: sessionmaker):
        crawler = RosterTransactionCrawler()
        data = [{"player_name": "mobile result"}]

        with (
            patch.object(
                crawler, "_crawl_mobile_page", new=AsyncMock(return_value=CrawlResult.success(data))
            ) as mobile,
            patch.object(crawler, "_crawl_desktop_page", new=AsyncMock()) as desktop,
            patch.object(crawler, "_save_to_db", return_value=(len(data), 0)) as save,
        ):
            result = await crawler.run(save=True, target_date="2025-06-15")

        assert result == data
        mobile.assert_awaited_once_with(date(2025, 6, 15))
        desktop.assert_not_awaited()
        save.assert_called_once_with(data, raise_on_error=False)

    @pytest.mark.asyncio
    async def test_run_falls_back_to_desktop_without_saving(self, ledger: sessionmaker):
        crawler = RosterTransactionCrawler()
        data = [{"player_name": "desktop result"}]

        with (
            patch.object(
                crawler,
                "_crawl_mobile_page",
                new=AsyncMock(
                    return_value=CrawlResult.failure(
                        CrawlOutcome.PERMANENT_ERROR,
                        error="boom",
                        error_code=FailureCode.FETCH_TIMEOUT.value,
                    ),
                ),
            ),
            patch.object(
                crawler, "_crawl_desktop_page", new=AsyncMock(return_value=CrawlResult.success(data))
            ) as desktop,
            patch.object(crawler, "_save_to_db", return_value=(len(data), 0)) as save,
        ):
            result = await crawler.run(target_date="2025-06-15")

        assert result == data
        desktop.assert_awaited_once_with(date(2025, 6, 15))
        save.assert_not_called()


SAMPLE_MOBILE_HTML = """
<html><body>
<div class="content">
  <h3>오늘자 선수 등록현황</h3>
  <strong class="team">LG</strong>
  <ul>
    <li><a href="/Player/Register.aspx?playerId=12345">김현수</a></li>
    <li><a href="/Player/Register.aspx?playerId=12346">박용택</a></li>
  </ul>
  <strong class="team">삼성</strong>
  <ul>
    <li><a href="/Player/Register.aspx?playerId=23456">이승엽</a></li>
  </ul>
  <h3>오늘자 선수 말소현황</h3>
  <strong class="team">한화</strong>
  <ul>
    <li><a href="/Player/Register.aspx?playerId=34567">이용찬</a></li>
  </ul>
</div>
</body></html>
"""

SAMPLE_ALTERNATE_HTML = """
<html><body>
<table>
  <tr><td class="team">LG</td></tr>
  <tr><td>등록 선수 현황</td></tr>
  <tr><td><a href="/Player/Register.aspx?playerId=99999">홍길동</a></td></tr>
  <tr><td>말소 선수 현황</td></tr>
  <tr><td><a href="/Player/Register.aspx?playerId=88888">김철수</a></td></tr>
</table>
</body></html>
"""


class TestParseMobileHtml:
    def setup_method(self):
        self.crawler = RosterTransactionCrawler()

    def test_parses_registered_and_deregistered(self):
        result = self.crawler._parse_mobile_html(SAMPLE_MOBILE_HTML, date(2025, 6, 15))
        assert len(result) == 4

        registered = [r for r in result if r["action"] == "registered"]
        deregistered = [r for r in result if r["action"] == "deregistered"]
        assert len(registered) == 3
        assert len(deregistered) == 1

    def test_registered_fields(self):
        result = self.crawler._parse_mobile_html(SAMPLE_MOBILE_HTML, date(2025, 6, 15))
        rec = result[0]
        assert rec["transaction_date"] == date(2025, 6, 15)
        assert rec["team_id"] == "LG"
        assert rec["player_id"] == 12345
        assert rec["player_name"] == "김현수"
        assert rec["action"] == "registered"
        assert rec["roster_level"] == "first_team"
        assert rec["inferred_to_level"] is None
        assert rec["source_type"] == "kbo_today_page"
        assert rec["confidence"] == "high"
        assert "dedupe_key" in rec

    def test_deregistered_inferred_level(self):
        result = self.crawler._parse_mobile_html(SAMPLE_MOBILE_HTML, date(2025, 6, 15))
        dereg = [r for r in result if r["action"] == "deregistered"][0]
        assert dereg["inferred_to_level"] == "second_team"
        assert dereg["team_id"] == "HH"

    def test_empty_html_returns_empty(self):
        result = self.crawler._parse_mobile_html("<html></html>", date(2025, 6, 15))
        assert result == []

    def test_player_without_id(self):
        html = """
        <html><body>
        <h3>오늘자 선수 등록현황</h3>
        <strong class="team">LG</strong>
        <ul>
          <li>홍길동</li>
        </ul>
        </body></html>
        """
        result = self.crawler._parse_mobile_html(html, date(2025, 6, 15))
        assert len(result) == 1
        assert result[0]["player_id"] is None
        assert result[0]["player_name"] == "홍길동"

    def test_skips_empty_player_names(self):
        html = """
        <html><body>
        <h3>오늘자 선수 등록현황</h3>
        <strong class="team">LG</strong>
        <ul>
          <li><a href="/Player/Register.aspx?playerId=111">  </a></li>
        </ul>
        </body></html>
        """
        result = self.crawler._parse_mobile_html(html, date(2025, 6, 15))
        assert len(result) == 0

    def test_unknown_team_blocks_yield_no_rows_but_are_reported(self):
        """The rows are still empty, but the block was seen and not understood.

        Callers must be able to tell that apart from a quiet day, which is why
        the analysis carries the team counts rather than the list alone.
        """
        html = """
        <h3>오늘자 선수 등록현황</h3>
        <strong class="team">알 수 없는 팀</strong>
        <ul><li><a href="?playerId=123">선수</a></li></ul>
        """

        assert self.crawler._parse_mobile_html(html, date(2025, 6, 15)) == []

        analysis = self.crawler._analyze_mobile_html(html, date(2025, 6, 15))
        assert analysis.structure_seen is True
        assert analysis.team_blocks == 1
        assert analysis.mapped_teams == 0
        assert analysis.unknown_teams == ["알 수 없는 팀"]


class TestParseAlternateMobile:
    def setup_method(self):
        self.crawler = RosterTransactionCrawler()

    def test_parses_alternate_layout(self):
        result = self.crawler._parse_alternate_mobile(SAMPLE_ALTERNATE_HTML, date(2025, 6, 15))
        assert len(result) == 2

    def test_alternate_registered(self):
        result = self.crawler._parse_alternate_mobile(SAMPLE_ALTERNATE_HTML, date(2025, 6, 15))
        reg = [r for r in result if r["action"] == "registered"]
        assert len(reg) == 1
        assert reg[0]["player_id"] == 99999
        assert reg[0]["player_name"] == "홍길동"

    def test_alternate_deregistered(self):
        result = self.crawler._parse_alternate_mobile(SAMPLE_ALTERNATE_HTML, date(2025, 6, 15))
        dereg = [r for r in result if r["action"] == "deregistered"]
        assert len(dereg) == 1
        assert dereg[0]["player_id"] == 88888
        assert dereg[0]["inferred_to_level"] == "second_team"

    def test_empty_alternate_returns_empty(self):
        result = self.crawler._parse_alternate_mobile("<html></html>", date(2025, 6, 15))
        assert result == []


class TestCrawlMobilePage:
    """The mobile fetch classifies its outcome instead of returning a bare list.

    Three states used to collapse into `[]`: a genuine quiet day, an HTTP
    failure, and a page that no longer has the expected structure. Only the first
    is data.
    """

    def _crawler(self, handler) -> RosterTransactionCrawler:
        client = CrawlerHttpClient(
            name=ROSTER_CRAWLER_NAME,
            policy=HttpPolicy(base_delay_seconds=0.0, max_attempts=1, max_backoff_seconds=0.0),
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
        return RosterTransactionCrawler(http_client=client)

    @pytest.mark.asyncio
    async def test_rows_produce_a_success_result(self):
        html = '<html>오늘자 선수 등록현황<strong class="team">LG</strong><ul><li>김현수</li></ul></html>'
        crawler = self._crawler(lambda request: httpx.Response(200, text=html))

        result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result.ok
        assert [row["player_name"] for row in result.data] == ["김현수"]
        assert crawler._raw_pages[0]["status_code"] == 200

    @pytest.mark.asyncio
    async def test_a_valid_page_with_no_rows_is_empty_not_a_failure(self):
        """The core of this track: a quiet day must not look like an outage."""
        html = "<html>오늘자 선수 등록현황<ul></ul></html>"
        crawler = self._crawler(lambda request: httpx.Response(200, text=html))

        result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result.outcome is CrawlOutcome.EMPTY
        assert result.error_code is None

    @pytest.mark.asyncio
    async def test_a_missing_section_is_a_selector_miss(self):
        crawler = self._crawler(lambda request: httpx.Response(200, text="<html>redesigned</html>"))

        result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result.error_code == FailureCode.PARSE_SELECTOR_MISSING.value

    @pytest.mark.asyncio
    async def test_only_unknown_team_names_is_a_normalization_failure(self):
        """A block we cannot read is a parser failure, not an empty day."""
        html = '<html>오늘자 선수 등록현황<strong class="team">어떤구단</strong><ul><li>홍길동</li></ul></html>'
        crawler = self._crawler(lambda request: httpx.Response(200, text=html))

        result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result.error_code == FailureCode.PARSE_SELECTOR_MISSING.value

    @pytest.mark.asyncio
    async def test_a_known_team_with_an_unknown_one_still_succeeds(self):
        html = (
            "<html>오늘자 선수 등록현황"
            '<strong class="team">LG</strong><ul><li>김현수</li></ul>'
            '<strong class="team">어떤구단</strong><ul><li>홍길동</li></ul>'
            "</html>"
        )
        crawler = self._crawler(lambda request: httpx.Response(200, text=html))

        result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result.ok
        assert [row["player_name"] for row in result.data] == ["김현수"]

    @pytest.mark.asyncio
    async def test_a_server_error_is_a_classified_fetch_failure(self):
        crawler = self._crawler(lambda request: httpx.Response(503, text="unavailable"))

        result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert crawler._raw_pages == []

    @pytest.mark.asyncio
    async def test_a_timeout_is_a_timeout(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow")

        result = await self._crawler(handler)._crawl_mobile_page(date(2025, 6, 15))

        assert result.error_code == FailureCode.FETCH_TIMEOUT.value

    @pytest.mark.asyncio
    async def test_a_rate_limit_is_not_a_generic_http_error(self):
        result = await self._crawler(lambda r: httpx.Response(429, text="slow down"))._crawl_mobile_page(
            date(2025, 6, 15),
        )

        assert result.error_code == FailureCode.FETCH_RATE_LIMITED.value


class TestResolveCrawl:
    """The fallback is for a source that cannot answer, not one with no data."""

    def _crawler(self) -> RosterTransactionCrawler:
        return RosterTransactionCrawler()

    def _failure(self, code: str) -> CrawlResult[list[dict]]:
        return CrawlResult.failure(CrawlOutcome.PERMANENT_ERROR, error="boom", error_code=code)

    @pytest.mark.asyncio
    async def test_a_quiet_day_never_triggers_the_fallback(self):
        crawler = self._crawler()
        crawler._crawl_mobile_page = AsyncMock(return_value=CrawlResult.empty())  # type: ignore[method-assign]
        crawler._crawl_desktop_page = AsyncMock()  # type: ignore[method-assign]

        result = await crawler._resolve_crawl(date(2025, 6, 15))

        assert result.outcome is CrawlOutcome.EMPTY
        crawler._crawl_desktop_page.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unreadable_page_triggers_the_fallback(self):
        crawler = self._crawler()
        rows = [{"player_name": "from desktop"}]
        crawler._crawl_mobile_page = AsyncMock(  # type: ignore[method-assign]
            return_value=self._failure(FailureCode.PARSE_SELECTOR_MISSING.value),
        )
        crawler._crawl_desktop_page = AsyncMock(  # type: ignore[method-assign]
            return_value=CrawlResult.success(rows),
        )

        result = await crawler._resolve_crawl(date(2025, 6, 15))

        assert result.ok
        assert result.data == rows

    @pytest.mark.asyncio
    async def test_a_fallback_empty_day_still_resolves_to_empty(self):
        """A confirmed quiet day on the fallback path is data, not a failure."""
        crawler = self._crawler()
        crawler._crawl_mobile_page = AsyncMock(  # type: ignore[method-assign]
            return_value=self._failure(FailureCode.FETCH_TIMEOUT.value),
        )
        crawler._crawl_desktop_page = AsyncMock(return_value=CrawlResult.empty())  # type: ignore[method-assign]

        result = await crawler._resolve_crawl(date(2025, 6, 15))

        assert result.outcome is CrawlOutcome.EMPTY
        assert result.error_code is None

    @pytest.mark.asyncio
    async def test_both_failing_keeps_the_primary_code(self):
        crawler = self._crawler()
        crawler._crawl_mobile_page = AsyncMock(  # type: ignore[method-assign]
            return_value=self._failure(FailureCode.FETCH_TIMEOUT.value),
        )
        crawler._crawl_desktop_page = AsyncMock(  # type: ignore[method-assign]
            return_value=self._failure(FailureCode.FETCH_BLOCKED.value),
        )

        result = await crawler._resolve_crawl(date(2025, 6, 15))

        assert result.error_code == FailureCode.FETCH_TIMEOUT.value
        assert FailureCode.FETCH_BLOCKED.value in (result.error or "")

    @pytest.mark.asyncio
    async def test_an_uninformative_primary_yields_to_a_real_fallback_code(self):
        crawler = self._crawler()
        crawler._crawl_mobile_page = AsyncMock(  # type: ignore[method-assign]
            return_value=self._failure(FailureCode.UNKNOWN.value),
        )
        crawler._crawl_desktop_page = AsyncMock(  # type: ignore[method-assign]
            return_value=self._failure(FailureCode.FETCH_RATE_LIMITED.value),
        )

        result = await crawler._resolve_crawl(date(2025, 6, 15))

        assert result.error_code == FailureCode.FETCH_RATE_LIMITED.value


class TestSaveToDb:
    def setup_method(self):
        self.crawler = RosterTransactionCrawler()
        self.crawler._raw_pages = [
            {"source_key": "kbo_today_roster", "url": "http://test", "html": "<html>", "status_code": 200},
        ]

    def test_save_commits_and_clears_raw_pages(self):
        data = [
            {"dedupe_key": "a", "player_name": "test1"},
            {"dedupe_key": "b", "player_name": "test2"},
        ]

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch(
            "src.crawlers.roster_transaction_crawler.SessionLocal",
            return_value=mock_session,
        ):
            with patch(
                "src.crawlers.roster_transaction_crawler.save_raw_snapshots",
                return_value=1,
            ):
                with patch("src.crawlers.roster_transaction_crawler.RosterTransactionRepository") as mock_repo_cls:
                    mock_repo = MagicMock()
                    mock_repo_cls.return_value = mock_repo
                    self.crawler._save_to_db(data)

        mock_session.commit.assert_called_once()
        assert self.crawler._raw_pages == []

    def test_save_rolls_back_on_error(self):
        data = [{"dedupe_key": "a"}]

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch(
            "src.crawlers.roster_transaction_crawler.SessionLocal",
            return_value=mock_session,
        ):
            with patch(
                "src.crawlers.roster_transaction_crawler.save_raw_snapshots",
                side_effect=RuntimeError("DB error"),
            ):
                self.crawler._save_to_db(data)

        mock_session.rollback.assert_called_once()

    def test_save_skips_duplicates(self):
        data = [
            {"dedupe_key": "a", "player_name": "dup"},
            {"dedupe_key": "a", "player_name": "dup"},
        ]

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch(
            "src.crawlers.roster_transaction_crawler.SessionLocal",
            return_value=mock_session,
        ):
            with patch(
                "src.crawlers.roster_transaction_crawler.save_raw_snapshots",
                return_value=0,
            ):
                with patch("src.crawlers.roster_transaction_crawler.RosterTransactionRepository") as mock_repo_cls:
                    mock_repo = MagicMock()
                    mock_repo_cls.return_value = mock_repo
                    self.crawler._save_to_db(data)

        assert mock_repo.save.call_count == 1

    def test_save_continues_after_individual_failure(self):
        data = [
            {"dedupe_key": "a", "player_name": "first"},
            {"dedupe_key": "b", "player_name": "second"},
        ]
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with (
            patch("src.crawlers.roster_transaction_crawler.SessionLocal", return_value=mock_session),
            patch("src.crawlers.roster_transaction_crawler.save_raw_snapshots", return_value=0),
            patch("src.crawlers.roster_transaction_crawler.RosterTransactionRepository") as mock_repo_cls,
        ):
            mock_repo = MagicMock()
            mock_repo.save.side_effect = [None, RuntimeError("invalid transaction")]
            mock_repo_cls.return_value = mock_repo
            self.crawler._save_to_db(data)

        assert mock_repo.save.call_count == 2
        mock_session.commit.assert_called_once()
        assert self.crawler._raw_pages == []


class TestDesktopCrawl:
    @pytest.mark.asyncio
    async def test_crawls_all_teams_releases_and_closes_owned_pool(self):
        crawler = RosterTransactionCrawler()
        page = MagicMock()
        page.goto = AsyncMock()
        page.evaluate = AsyncMock()
        page.wait_for_timeout = AsyncMock()
        page.content = AsyncMock(return_value="<html>desktop</html>")
        page.expect_response = MagicMock(return_value=FakeResponseContext())

        pool = MagicMock()
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()
        team_results = [[{"player_id": 1, "player_name": "first"}], ValueError("team failed"), *([[]] * 8)]

        with (
            patch("src.crawlers.roster_transaction_crawler.AsyncPlaywrightPool", return_value=pool),
            patch.object(crawler, "_extract_desktop_roster", new=AsyncMock(side_effect=team_results)),
        ):
            result = await crawler._crawl_desktop_page(date(2025, 6, 15))

        assert result.ok
        assert len(result.data) == 1
        pool.start.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_awaited_once()
        assert crawler._raw_pages[0]["source_key"] == "kbo_player_register"

    @pytest.mark.asyncio
    async def test_continues_when_calendar_response_times_out(self):
        crawler = RosterTransactionCrawler()
        page = MagicMock()
        page.goto = AsyncMock()
        page.evaluate = AsyncMock()
        page.wait_for_timeout = AsyncMock()
        page.content = AsyncMock(return_value="<html>desktop</html>")
        page.expect_response = MagicMock(return_value=FakeResponseContext(raises_timeout=True))

        pool = MagicMock()
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()

        with (
            patch("src.crawlers.roster_transaction_crawler.AsyncPlaywrightPool", return_value=pool),
            patch.object(crawler, "_extract_desktop_roster", new=AsyncMock(return_value=[])),
        ):
            result = await crawler._crawl_desktop_page(date(2025, 6, 15))

        # Every team yielded no rows without raising: a confirmed quiet desktop day.
        assert result.outcome is CrawlOutcome.EMPTY
        assert result.error_code is None
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_extract_desktop_roster_builds_transaction(self):
        crawler = RosterTransactionCrawler()
        page = MagicMock()
        page.evaluate = AsyncMock(return_value=[{"player_id": "123", "player_name": "홍길동"}])

        result = await crawler._extract_desktop_roster(page, "LG", date(2025, 6, 15))

        assert result == [
            {
                "transaction_date": date(2025, 6, 15),
                "team_id": "LG",
                "player_id": 123,
                "player_name": "홍길동",
                "action": "registered",
                "roster_level": "first_team",
                "source_type": "kbo_today_page",
                "confidence": "high",
                "dedupe_key": "2025-06-15_LG_홍길동_registered",
            },
        ]
