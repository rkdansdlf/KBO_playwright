from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark

from src.crawlers.player_movement_crawler import PlayerMovementCrawler


@pytest.fixture
def crawler():
    return PlayerMovementCrawler()


class TestExtractTable:
    @mark.asyncio
    async def test_returns_empty_when_no_rows(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(return_value=[])
        result = await crawler._extract_table(mock_page)
        assert result == []

    @mark.asyncio
    async def test_filters_empty_date_rows(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(
            return_value=[
                {"date": "2024-03-15", "section": "Trade", "team_code": "LG", "player_name": "Kim", "remarks": ""},
                {"date": "", "section": "Trade", "team_code": "SS", "player_name": "Park", "remarks": ""},
                {"date": "2024-04-01", "section": "", "team_code": "NC", "player_name": "Lee", "remarks": ""},
            ],
        )
        result = await crawler._extract_table(mock_page)
        assert len(result) == 1
        assert result[0]["player_name"] == "Kim"

    @mark.asyncio
    async def test_returns_valid_data(self, crawler):
        mock_page = MagicMock()
        mock_page.evaluate = AsyncMock(
            return_value=[
                {"date": "2024-03-15", "section": "Trade", "team_code": "LG", "player_name": "Kim", "remarks": "cash"},
                {"date": "2024-04-01", "section": "FA", "team_code": "SS", "player_name": "Park", "remarks": ""},
            ],
        )
        result = await crawler._extract_table(mock_page)
        assert len(result) == 2


class TestCrawlYear:
    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncRetrying")
    async def test_calls_extract_table_and_paginates(self, mock_retrying_cls, crawler):
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")

        mock_retrying = MagicMock()
        mock_retrying.__aiter__.return_value = [MagicMock()]
        mock_retrying_cls.return_value = mock_retrying

        crawler._extract_table = AsyncMock(
            side_effect=[
                [{"date": "2024-03-15", "section": "Trade", "team_code": "LG", "player_name": "Kim", "remarks": ""}],
                [],
            ],
        )
        mock_page.get_by_role.return_value.count = AsyncMock(return_value=0)
        mock_page.locator.return_value.count = AsyncMock(return_value=0)

        result = await crawler._crawl_year(mock_page, 2024)

        assert len(result) == 1
        mock_page.select_option.assert_called_with("#selYear", "2024")
        mock_page.click.assert_called_with("#btnSearch")


class TestCrawlYears:
    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_crawls_year_range(self, mock_pool_cls, crawler):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")

        crawler._extract_table = AsyncMock(
            side_effect=[
                [{"date": "2023-01-01", "section": "Trade", "team_code": "LG", "player_name": "A", "remarks": ""}],
                [{"date": "2024-01-01", "section": "FA", "team_code": "SS", "player_name": "B", "remarks": ""}],
            ],
        )
        mock_page.get_by_role.return_value.count = AsyncMock(return_value=0)
        mock_page.locator.return_value.count = AsyncMock(return_value=0)

        result = await crawler.crawl_years(2023, 2024)

        assert len(result) == 2
        mock_pool.close.assert_awaited_once()

    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_cleans_up_pool_on_exception(self, mock_pool_cls, crawler):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")

        crawler._extract_table = AsyncMock(side_effect=RuntimeError("boom"))

        result = await crawler.crawl_years(2023, 2023)

        assert result == []
        mock_pool.close.assert_awaited_once()

    @mark.asyncio
    @patch("src.crawlers.player_movement_crawler.save_raw_snapshots", return_value=1)
    @patch("src.crawlers.player_movement_crawler.SessionLocal")
    @patch("src.crawlers.player_movement_crawler.AsyncPlaywrightPool")
    async def test_save_snapshots_tracks_kbo_player_movement_source(
        self,
        mock_pool_cls,
        mock_session_cls,
        mock_save_raw_snapshots,
        crawler,
    ):
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool
        mock_pool.start = AsyncMock()
        mock_pool.release = AsyncMock()
        mock_pool.close = AsyncMock()
        mock_page = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_page.goto = AsyncMock()
        mock_page.select_option = AsyncMock()
        mock_page.click = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.content = AsyncMock(return_value="<html>movement</html>")
        mock_page.get_by_role.return_value.count = AsyncMock(return_value=0)
        mock_page.locator.return_value.count = AsyncMock(return_value=0)
        crawler._extract_table = AsyncMock(return_value=[])

        mock_session = MagicMock()
        mock_session_cls.return_value.__enter__.return_value = mock_session

        await crawler.crawl_years(2025, 2025, save_snapshots=True)

        pages = mock_save_raw_snapshots.call_args.args[1]
        assert pages[0]["source_key"] == "kbo_player_movement"
        assert pages[0]["status_code"] == 200
        assert len(pages) == 2
        mock_session.commit.assert_called_once()
