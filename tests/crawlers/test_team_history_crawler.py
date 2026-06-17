from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.team_history_crawler import TeamHistoryCrawler


@pytest.fixture
def crawler():
    return TeamHistoryCrawler()


class TestStartAndClose:
    @mark.asyncio
    @patch("src.crawlers.team_history_crawler.async_playwright")
    async def test_start_creates_browser_and_page(self, mock_async_pw, crawler):
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_async_pw.return_value.start = AsyncMock(return_value=mock_pw)
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_context.new_page = AsyncMock(return_value=mock_page)

        await crawler.start()

        assert crawler.browser is mock_browser
        assert crawler.page is mock_page
        mock_pw.chromium.launch.assert_awaited_once_with(headless=True)

    @mark.asyncio
    async def test_close_cleans_up_resources(self, crawler):
        crawler.context = AsyncMock()
        crawler.browser = AsyncMock()
        crawler.playwright = AsyncMock()

        await crawler.close()

        crawler.context.close.assert_awaited_once()
        crawler.browser.close.assert_awaited_once()
        crawler.playwright.stop.assert_awaited_once()


class TestCrawl:
    @mark.asyncio
    @patch.object(TeamHistoryCrawler, "start", new=AsyncMock())
    async def test_returns_history_data(self, crawler):
        page = MagicMock()
        page.goto = AsyncMock()
        page.content = AsyncMock(return_value="<html></html>")
        crawler.page = page

        year_th = MagicMock()
        year_th.count = AsyncMock(return_value=1)
        year_th.inner_text = AsyncMock(return_value="2024")

        # Build a cell-like locator that has async .all returning one cell
        cell = MagicMock()
        cell_nums = MagicMock()
        cell_nums.count = AsyncMock(return_value=0)
        cell_img = MagicMock()
        cell_img.count = AsyncMock(return_value=0)
        cell_name = MagicMock()
        cell_name.count = AsyncMock(return_value=0)
        cell.locator.side_effect = lambda sel: {
            "span.nums": cell_nums,
            "img": cell_img,
            "span:not(.nums)": cell_name,
        }.get(sel, MagicMock())

        td_loc = MagicMock()
        td_loc.all = AsyncMock(return_value=[cell])
        mock_row = MagicMock()
        mock_row.locator.side_effect = lambda sel: year_th if sel == "th" else td_loc

        page.locator.return_value.all = AsyncMock(return_value=[mock_row, mock_row])

        with patch.object(crawler, "_raw_pages", []):
            result = await crawler.crawl()

        assert isinstance(result, list)

    @mark.asyncio
    @patch.object(TeamHistoryCrawler, "start", new=AsyncMock())
    async def test_handles_empty_rows(self, crawler):
        page = MagicMock()
        page.goto = AsyncMock()
        page.content = AsyncMock(return_value="<html></html>")
        page.locator.return_value.all = AsyncMock(return_value=[])
        crawler.page = page

        result = await crawler.crawl()
        assert result == []


class TestSave:
    @mark.asyncio
    @patch("src.crawlers.team_history_crawler.SessionLocal")
    @patch("src.crawlers.team_history_crawler.save_raw_snapshots")
    @patch("src.crawlers.team_history_crawler.select")
    async def test_saves_history_entries(self, mock_select, mock_snap, mock_sl, crawler):
        mock_session = MagicMock()
        mock_sl.return_value.__enter__.return_value = mock_session
        mock_snap.return_value = 1
        mock_team = MagicMock()
        mock_team.team_id = "LG"
        mock_team.franchise_id = "FR001"
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_team]
        mock_session.execute.return_value = mock_result
        mock_session.execute.return_value.scalars.return_value.first.return_value = None

        crawler._raw_pages = [{"url": "test", "html": "<html/>"}]
        data = [
            {"season": 2024, "team_name": "LG Twins", "logo_url": "/logo.png", "ranking": 1, "slot_index": 0},
        ]

        await crawler.save(data)

        mock_session.commit.assert_called_once()
        assert crawler._raw_pages == []

    @mark.asyncio
    @patch("src.crawlers.team_history_crawler.SessionLocal")
    async def test_catches_unsaved_data_safely(self, mock_sl, crawler):
        mock_session = MagicMock()
        mock_sl.return_value.__enter__.return_value = mock_session
        mock_session.execute.side_effect = SQLAlchemyError("db error")

        await crawler.save([{"season": 2024, "team_name": "Unknown", "ranking": 99}])
        mock_session.rollback.assert_called_once()
