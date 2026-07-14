from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import src.crawlers.ticket_crawler as ticket_module
from src.crawlers.ticket_crawler import TEAM_TICKET_INFO, TicketCrawler


class FakeAsyncClient:
    def __init__(self, response):
        self.response = response
        self.urls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url):
        self.urls.append(url)
        return self.response


class ErrorAsyncClient(FakeAsyncClient):
    async def get(self, url):
        raise httpx.HTTPError("request failed")


class TestAltToTeamCode:
    def setup_method(self):
        self.crawler = TicketCrawler()

    def test_known_team_alts(self):
        assert self.crawler._alt_to_team_code("LG") == "LG"
        assert self.crawler._alt_to_team_code("kt") == "KT"
        assert self.crawler._alt_to_team_code("두산") == "OB"
        assert self.crawler._alt_to_team_code("롯데") == "LT"
        assert self.crawler._alt_to_team_code("키움") == "WO"
        assert self.crawler._alt_to_team_code("ssg") == "SK"

    def test_no_match_returns_none(self):
        assert self.crawler._alt_to_team_code("없는팀") is None

    def test_case_insensitive(self):
        assert self.crawler._alt_to_team_code("LG") == "LG"
        assert self.crawler._alt_to_team_code("lg") == "LG"


class TestTeamCodeToKr:
    def setup_method(self):
        self.crawler = TicketCrawler()

    def test_known_codes(self):
        assert self.crawler._team_code_to_kr("LG") == "LG"
        assert self.crawler._team_code_to_kr("HH") == "한화"
        assert self.crawler._team_code_to_kr("HT") == "KIA"

    def test_unknown_returns_none(self):
        assert self.crawler._team_code_to_kr("ZZ") is None


class TestBuildOpenRules:
    def setup_method(self):
        self.crawler = TicketCrawler()

    def test_returns_all_teams(self):
        rules = self.crawler._build_open_rules()
        assert len(rules) == 10

    def test_rule_structure(self):
        rules = self.crawler._build_open_rules()
        lg = [r for r in rules if r["team_id"] == "LG"][0]
        assert lg["platform"] == "Ticketlink"
        assert lg["open_offset_days"] == 7

    def test_all_have_required_keys(self):
        rules = self.crawler._build_open_rules()
        for r in rules:
            assert "team_id" in r
            assert "platform" in r
            assert "open_offset_days" in r
            assert "open_time" in r


class TestTicketSourceKeyMap:
    def test_all_teams_mapped(self):
        crawler = TicketCrawler()
        for code in TEAM_TICKET_INFO:
            assert code in crawler.TICKET_SOURCE_KEY_MAP

    def test_source_keys_unique(self):
        crawler = TicketCrawler()
        values = list(crawler.TICKET_SOURCE_KEY_MAP.values())
        assert len(values) == len(set(values))


class TestSaveToDb:
    def setup_method(self):
        self.crawler = TicketCrawler()
        self.crawler._raw_pages = [
            {"source_key": "kbo_ticket_map", "url": "http://test", "html": "<html>", "status_code": 200},
        ]

    def test_save_prices_and_rules(self):
        prices = [{"seat_type": "A", "price": 10000}, {"seat_type": "B", "price": 5000}]
        rules = [{"team_id": "LG", "platform": "Ticketlink"}]

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch(
            "src.crawlers.ticket_crawler.SessionLocal",
            return_value=mock_session,
        ):
            with patch(
                "src.crawlers.ticket_crawler.save_raw_snapshots",
                return_value=1,
            ):
                with patch("src.crawlers.ticket_crawler.TicketPriceRepository") as mock_price_cls:
                    with patch("src.crawlers.ticket_crawler.TicketOpenRuleRepository") as mock_rule_cls:
                        mock_price = MagicMock()
                        mock_rule = MagicMock()
                        mock_price_cls.return_value = mock_price
                        mock_rule_cls.return_value = mock_rule
                        self.crawler._save_to_db(prices, rules)

        assert mock_price.save.call_count == 2
        assert mock_rule.save.call_count == 1
        mock_session.commit.assert_called_once()
        assert self.crawler._raw_pages == []

    def test_save_rolls_back_on_error(self):
        prices = [{"seat_type": "A"}]
        rules = [{"team_id": "LG"}]

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch(
            "src.crawlers.ticket_crawler.SessionLocal",
            return_value=mock_session,
        ):
            with patch(
                "src.crawlers.ticket_crawler.save_raw_snapshots",
                side_effect=RuntimeError("DB error"),
            ):
                self.crawler._save_to_db(prices, rules)

        mock_session.rollback.assert_called_once()

    def test_save_handles_individual_price_failure(self):
        prices = [{"seat_type": "A"}, {"seat_type": "B"}]
        rules = []

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch(
            "src.crawlers.ticket_crawler.SessionLocal",
            return_value=mock_session,
        ):
            with patch(
                "src.crawlers.ticket_crawler.save_raw_snapshots",
                return_value=0,
            ):
                with patch("src.crawlers.ticket_crawler.TicketPriceRepository") as mock_price_cls:
                    with patch("src.crawlers.ticket_crawler.TicketOpenRuleRepository"):
                        mock_price = MagicMock()
                        mock_price.save.side_effect = [None, RuntimeError("fail")]
                        mock_price_cls.return_value = mock_price
                        self.crawler._save_to_db(prices, rules)

        assert mock_price.save.call_count == 2
        mock_session.commit.assert_called_once()

    def test_save_handles_individual_rule_failure(self):
        rules = [{"team_id": "LG"}, {"team_id": "HH"}]
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with (
            patch("src.crawlers.ticket_crawler.SessionLocal", return_value=mock_session),
            patch("src.crawlers.ticket_crawler.save_raw_snapshots", return_value=0),
            patch("src.crawlers.ticket_crawler.TicketPriceRepository"),
            patch("src.crawlers.ticket_crawler.TicketOpenRuleRepository") as mock_rule_cls,
        ):
            mock_rule = MagicMock()
            mock_rule.save.side_effect = [None, RuntimeError("invalid rule")]
            mock_rule_cls.return_value = mock_rule
            self.crawler._save_to_db([], rules)

        assert mock_rule.save.call_count == 2
        mock_session.commit.assert_called_once()
        assert self.crawler._raw_pages == []


class TestCrawlKboTicketMap:
    @pytest.mark.asyncio
    async def test_ticket_map_html_updates_team_ticket_urls(self):
        original_info = deepcopy(TEAM_TICKET_INFO)
        try:
            TEAM_TICKET_INFO["HH"]["ticket_url"] = None
            TEAM_TICKET_INFO["SS"]["ticket_url"] = None
            lg_url = TEAM_TICKET_INFO["LG"]["ticket_url"]

            html = """
            <html><body>
              <ul class="teamView">
                <li><a href="//ticket.example.com/hanwha"><img alt="한화 이글스"></a></li>
                <li><a href="https://ticket.example.com/samsung"><img alt="삼성 라이온즈"></a></li>
                <li><a href="https://ticket.example.com/lg"><img alt="LG 트윈스"></a></li>
                <li><a href="https://ticket.example.com/unknown"><img alt="없는 팀"></a></li>
              </ul>
            </body></html>
            """
            response = MagicMock(status_code=200, text=html)
            fake_client = FakeAsyncClient(response)
            crawler = TicketCrawler()

            with patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=fake_client):
                with patch("src.crawlers.ticket_crawler.throttle.wait", new=AsyncMock()):
                    with patch.object(
                        crawler, "_crawl_team_ticket_pages", new=AsyncMock(return_value=[{"seat_type": "fixture"}])
                    ):
                        result = await crawler._crawl_kbo_ticket_map()

            assert result == [{"seat_type": "fixture"}]
            assert TEAM_TICKET_INFO["HH"]["ticket_url"] == "https://ticket.example.com/hanwha"
            assert TEAM_TICKET_INFO["SS"]["ticket_url"] == "https://ticket.example.com/samsung"
            assert TEAM_TICKET_INFO["LG"]["ticket_url"] == lg_url
            assert crawler._raw_pages[0]["source_key"] == "kbo_ticket_map"
            assert crawler._raw_pages[0]["html"] == html
        finally:
            TEAM_TICKET_INFO.clear()
            TEAM_TICKET_INFO.update(original_info)

    @pytest.mark.asyncio
    async def test_non_ok_response_returns_empty(self):
        crawler = TicketCrawler()

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = ""

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=mock_client):
            with patch("src.crawlers.ticket_crawler.throttle.wait"):
                result = await crawler._crawl_kbo_ticket_map()

        assert result == []

    @pytest.mark.asyncio
    async def test_http_error_returns_empty(self):
        crawler = TicketCrawler()

        with (
            patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=ErrorAsyncClient(None)),
            patch("src.crawlers.ticket_crawler.throttle.wait", new=AsyncMock()),
        ):
            result = await crawler._crawl_kbo_ticket_map()

        assert result == []

    @pytest.mark.asyncio
    async def test_missing_team_view_falls_back_to_lg_page(self):
        crawler = TicketCrawler()
        response = MagicMock(status_code=200, text="<html></html>")
        lg_prices = [{"seat_type": "LG"}]

        with (
            patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=FakeAsyncClient(response)),
            patch("src.crawlers.ticket_crawler.throttle.wait", new=AsyncMock()),
            patch.object(crawler, "_crawl_team_ticket_pages", new=AsyncMock(return_value=[])),
            patch.object(crawler, "_crawl_lg_ticket_page", new=AsyncMock(return_value=lg_prices)) as lg_page,
        ):
            result = await crawler._crawl_kbo_ticket_map()

        assert result == lg_prices
        lg_page.assert_awaited_once()


class TestRun:
    @pytest.mark.asyncio
    async def test_run_keeps_map_prices_without_lg_fallback(self):
        crawler = TicketCrawler()
        prices = [{"seat_type": "map"}]

        with (
            patch.object(crawler, "_crawl_kbo_ticket_map", new=AsyncMock(return_value=prices)) as map_page,
            patch.object(crawler, "_crawl_lg_ticket_page", new=AsyncMock()) as lg_page,
        ):
            result = await crawler.run()

        assert result == prices
        map_page.assert_awaited_once()
        lg_page.assert_not_awaited()
        assert crawler.current_season >= 2000


class TestCrawlTeamTicketPages:
    @pytest.mark.asyncio
    async def test_crawls_available_pages_skips_missing_and_non_ok(self):
        crawler = TicketCrawler()
        responses = [
            FakeAsyncClient(MagicMock(status_code=200, text="good html")),
            FakeAsyncClient(MagicMock(status_code=503, text="bad html")),
            ErrorAsyncClient(None),
        ]
        team_info = {
            "LG": {"ticket_url": "https://lg.example"},
            "HH": {"ticket_url": None},
            "SS": {"ticket_url": "https://ss.example"},
            "KT": {"ticket_url": "https://kt.example"},
            "HT": {"ticket_url": "https://ht.example"},
        }

        with (
            patch.object(ticket_module, "TEAM_TICKET_INFO", team_info),
            patch("src.crawlers.ticket_crawler.httpx.AsyncClient", side_effect=responses),
            patch("src.crawlers.ticket_crawler.throttle.wait", new=AsyncMock()),
            patch("src.crawlers.ticket_crawler.parse_ticket_page", return_value=[{"seat_type": "SS"}]) as parse,
        ):
            result = await crawler._crawl_team_ticket_pages()

        assert result == [{"seat_type": "SS"}]
        parse.assert_called_once_with("good html", "samsung_lions_ticket", {"season": crawler.current_season})
        assert [page["source_key"] for page in crawler._raw_pages] == ["samsung_lions_ticket"]


class TestCrawlLgTicketPage:
    @pytest.mark.asyncio
    async def test_success_parses_and_records_lg_page(self):
        crawler = TicketCrawler()
        response = MagicMock(status_code=200, text="lg html")

        with (
            patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=FakeAsyncClient(response)),
            patch("src.crawlers.ticket_crawler.throttle.wait", new=AsyncMock()),
            patch("src.crawlers.ticket_crawler.parse_ticket_page", return_value=[{"seat_type": "LG"}]) as parse,
        ):
            result = await crawler._crawl_lg_ticket_page()

        assert result == [{"seat_type": "LG"}]
        parse.assert_called_once_with("lg html", "lg_twins_ticket", {"season": crawler.current_season})
        assert crawler._raw_pages[0]["source_key"] == "lg_twins_ticket"

    @pytest.mark.asyncio
    async def test_missing_lg_url_returns_empty(self):
        crawler = TicketCrawler()

        with patch.object(ticket_module, "TEAM_TICKET_INFO", {"LG": {"ticket_url": None}}):
            result = await crawler._crawl_lg_ticket_page()

        assert result == []

    @pytest.mark.asyncio
    async def test_non_ok_lg_response_returns_empty(self):
        crawler = TicketCrawler()
        response = MagicMock(status_code=404, text="not found")

        with (
            patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=FakeAsyncClient(response)),
            patch("src.crawlers.ticket_crawler.throttle.wait", new=AsyncMock()),
        ):
            result = await crawler._crawl_lg_ticket_page()

        assert result == []

    @pytest.mark.asyncio
    async def test_http_error_lg_response_returns_empty(self):
        crawler = TicketCrawler()

        with (
            patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=ErrorAsyncClient(None)),
            patch("src.crawlers.ticket_crawler.throttle.wait", new=AsyncMock()),
        ):
            result = await crawler._crawl_lg_ticket_page()

        assert result == []

    @pytest.mark.asyncio
    async def test_run_returns_prices(self):
        crawler = TicketCrawler()
        with patch.object(crawler, "_crawl_kbo_ticket_map", return_value=[]):
            with patch.object(crawler, "_crawl_lg_ticket_page", return_value=[]):
                result = await crawler.run(save=False, season=2025)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_run_with_save(self):
        crawler = TicketCrawler()
        with patch.object(crawler, "_crawl_kbo_ticket_map", return_value=[{"seat": "A"}]):
            with patch.object(crawler, "_crawl_lg_ticket_page", return_value=[]):
                with patch.object(crawler, "_save_to_db") as mock_save:
                    result = await crawler.run(save=True, season=2025)
        mock_save.assert_called_once()
        assert len(result) == 1
