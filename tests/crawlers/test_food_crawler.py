from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.food_crawler import TEAM_FOOD_SOURCES, FoodCrawler


class TestParseFoodPage:
    def setup_method(self):
        self.crawler = FoodCrawler()

    def test_parses_menu_with_price(self):
        html = "<html><body>더블치즈버거: 8,500원</body></html>"
        info = {"stadium_id": "JAMSIL"}
        result = self.crawler._parse_food_page(html, info)
        assert len(result) == 1
        menus = result[0]["menus"]
        assert len(menus) >= 1
        assert menus[0]["menu_name"] == "더블치즈버거"
        assert menus[0]["price"] == 8500

    def test_no_price_no_result(self):
        html = "<html><body>메뉴 정보가 없습니다.</body></html>"
        info = {"stadium_id": "JAMSIL"}
        result = self.crawler._parse_food_page(html, info)
        assert result == []

    def test_vendor_metadata(self):
        html = "<html><body>떡볶이 3,000원</body></html>"
        info = {"stadium_id": "SAJIK"}
        result = self.crawler._parse_food_page(html, info)
        assert result[0]["vendor"]["stadium_id"] == "SAJIK"
        assert result[0]["vendor"]["order_method"] == "onsite"


def test_food_sources_cover_seeded_refresh_sources():
    assert TEAM_FOOD_SOURCES["ALL"]["source_key"] == "gujangfood_com"
    assert TEAM_FOOD_SOURCES["NC"]["url"] == "https://www.ncdinos.com/dinos/stadium.do"


class TestFoodCrawlerOperations:
    @pytest.mark.asyncio
    async def test_crawl_team_fetches_vendors_and_records_snapshot(self):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(return_value=MagicMock(status_code=200, text="떡볶이 3,000원"))
        crawler = FoodCrawler()
        info = TEAM_FOOD_SOURCES["LT"]

        with (
            patch("src.crawlers.food_crawler.httpx.AsyncClient", return_value=client),
            patch("src.crawlers.food_crawler.throttle.wait", new=AsyncMock()) as wait,
        ):
            vendors = await crawler._crawl_team_food("LT", info)

        wait.assert_awaited_once_with("www.giantsclub.com")
        assert vendors[0]["vendor"]["stadium_id"] == "SAJIK"
        assert crawler._raw_pages[0]["source_key"] == "lotte_giants_fnb"

    @pytest.mark.asyncio
    async def test_crawl_team_returns_empty_for_non_ok_response(self):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(return_value=MagicMock(status_code=503))

        with (
            patch("src.crawlers.food_crawler.httpx.AsyncClient", return_value=client),
            patch("src.crawlers.food_crawler.throttle.wait", new=AsyncMock()),
        ):
            vendors = await FoodCrawler()._crawl_team_food("LT", TEAM_FOOD_SOURCES["LT"])

        assert vendors == []

    @pytest.mark.asyncio
    async def test_run_saves_filtered_team_results(self):
        crawler = FoodCrawler()
        crawler._crawl_team_food = AsyncMock(return_value=[{"vendor": {"vendor_name": "매점"}}])
        crawler._save_to_db = MagicMock()

        records = await crawler.run(save=True, team_filter="NC")

        assert len(records) == 1
        crawler._crawl_team_food.assert_awaited_once_with("NC", TEAM_FOOD_SOURCES["NC"])
        crawler._save_to_db.assert_called_once_with(records)

    def test_save_to_db_persists_vendor_and_menu(self):
        session = MagicMock()
        vendor_repo = MagicMock()
        vendor_repo.save.return_value = MagicMock(id=7)
        menu_repo = MagicMock()
        crawler = FoodCrawler()
        crawler._raw_pages = [{"source_key": "lotte_giants_fnb"}]
        entry = {"vendor": {"vendor_name": "매점"}, "menus": [{"menu_name": "떡볶이", "price": 3000}]}

        with (
            patch("src.crawlers.food_crawler.SessionLocal") as session_local,
            patch("src.crawlers.food_crawler.save_raw_snapshots", return_value=1),
            patch("src.crawlers.food_crawler.StadiumFoodVendorRepository", return_value=vendor_repo),
            patch("src.crawlers.food_crawler.StadiumFoodMenuItemRepository", return_value=menu_repo),
        ):
            session_local.return_value.__enter__.return_value = session
            crawler._save_to_db([entry])

        menu_repo.save.assert_called_once_with({"vendor_id": 7, "menu_name": "떡볶이", "price": 3000})
        session.commit.assert_called_once()
        assert crawler._raw_pages == []
