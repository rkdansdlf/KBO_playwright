from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler


class FakeAsyncClient:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url):
        return self.response


class ErrorAsyncClient(FakeAsyncClient):
    async def get(self, url):
        raise httpx.HTTPError("request failed")


class FakeResponseContext:
    def __init__(self, *, raises_timeout=False):
        self.raises_timeout = raises_timeout

    async def __aenter__(self):
        if self.raises_timeout:
            raise TimeoutError("response wait timed out")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


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
    async def test_run_uses_mobile_results_and_saves(self):
        crawler = RosterTransactionCrawler()
        data = [{"player_name": "mobile result"}]

        with (
            patch.object(crawler, "_crawl_mobile_page", new=AsyncMock(return_value=data)) as mobile,
            patch.object(crawler, "_crawl_desktop_page", new=AsyncMock()) as desktop,
            patch.object(crawler, "_save_to_db") as save,
        ):
            result = await crawler.run(save=True, target_date="2025-06-15")

        assert result == data
        mobile.assert_awaited_once_with(date(2025, 6, 15))
        desktop.assert_not_awaited()
        save.assert_called_once_with(data)

    @pytest.mark.asyncio
    async def test_run_falls_back_to_desktop_without_saving(self):
        crawler = RosterTransactionCrawler()
        data = [{"player_name": "desktop result"}]

        with (
            patch.object(crawler, "_crawl_mobile_page", new=AsyncMock(return_value=[])),
            patch.object(crawler, "_crawl_desktop_page", new=AsyncMock(return_value=data)) as desktop,
            patch.object(crawler, "_save_to_db") as save,
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

    def test_skips_unknown_team_blocks(self):
        html = """
        <h3>오늘자 선수 등록현황</h3>
        <strong class="team">알 수 없는 팀</strong>
        <ul><li><a href="?playerId=123">선수</a></li></ul>
        """

        assert self.crawler._parse_mobile_html(html, date(2025, 6, 15)) == []


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
    @pytest.mark.asyncio
    async def test_success_records_raw_page_and_parses_html(self):
        crawler = RosterTransactionCrawler()
        response = MagicMock(status_code=200, text="<html>mobile</html>")
        parsed = [{"player_name": "parsed"}]

        with (
            patch("src.crawlers.roster_transaction_crawler.httpx.AsyncClient", return_value=FakeAsyncClient(response)),
            patch("src.crawlers.roster_transaction_crawler.throttle.wait", new=AsyncMock()),
            patch.object(crawler, "_parse_mobile_html", return_value=parsed) as parse,
        ):
            result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result == parsed
        parse.assert_called_once_with("<html>mobile</html>", date(2025, 6, 15))
        assert crawler._raw_pages == [
            {
                "source_key": "kbo_today_roster",
                "url": "https://m.koreabaseball.com/Kbo/PlayerAdd.aspx?searchDate=2025-06-15",
                "html": "<html>mobile</html>",
                "status_code": 200,
            },
        ]

    @pytest.mark.asyncio
    async def test_non_ok_response_returns_empty(self):
        crawler = RosterTransactionCrawler()
        response = MagicMock(status_code=503, text="unavailable")

        with (
            patch("src.crawlers.roster_transaction_crawler.httpx.AsyncClient", return_value=FakeAsyncClient(response)),
            patch("src.crawlers.roster_transaction_crawler.throttle.wait", new=AsyncMock()),
        ):
            result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result == []
        assert crawler._raw_pages == []

    @pytest.mark.asyncio
    async def test_http_error_returns_empty(self):
        crawler = RosterTransactionCrawler()

        with (
            patch("src.crawlers.roster_transaction_crawler.httpx.AsyncClient", return_value=ErrorAsyncClient(None)),
            patch("src.crawlers.roster_transaction_crawler.throttle.wait", new=AsyncMock()),
        ):
            result = await crawler._crawl_mobile_page(date(2025, 6, 15))

        assert result == []


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

        assert len(result) == 1
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

        assert result == []
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
