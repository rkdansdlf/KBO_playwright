from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.ticket_crawler import TEAM_TICKET_INFO, TicketCrawler


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
            {"source_key": "kbo_ticket_map", "url": "http://test", "html": "<html>", "status_code": 200}
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


class TestCrawlKboTicketMap:
    @pytest.mark.asyncio
    async def test_non_ok_response_returns_empty(self):
        from unittest.mock import AsyncMock

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
        from unittest.mock import AsyncMock
        import httpx

        crawler = TicketCrawler()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=httpx.HTTPError("fail"))

        with patch("src.crawlers.ticket_crawler.httpx.AsyncClient", return_value=mock_client):
            with patch("src.crawlers.ticket_crawler.throttle.wait"):
                result = await crawler._crawl_kbo_ticket_map()

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
