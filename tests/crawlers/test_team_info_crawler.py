from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark

from src.crawlers.team_info_crawler import TeamInfoCrawler


@pytest.fixture
def crawler():
    return TeamInfoCrawler()


class TestStartAndClose:
    @mark.asyncio
    @patch("src.crawlers.team_info_crawler.async_playwright")
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
    def _setup_nth_loc(self, cols, link_count=0):
        """Helper to set up nth_loc with proper locator chain including link."""
        loc_rv = MagicMock()
        loc_rv.all = AsyncMock(return_value=cols)
        loc_rv.count = AsyncMock(return_value=link_count)

        link = MagicMock()
        link.count = AsyncMock(return_value=0)
        loc_rv.nth.return_value.locator.return_value.first = link

        nth_loc = MagicMock()
        nth_loc.locator.return_value = loc_rv
        return nth_loc, link

    @mark.asyncio
    @patch.object(TeamInfoCrawler, "start", new=AsyncMock())
    @patch("src.crawlers.team_info_crawler.throttle.wait", new=AsyncMock())
    async def test_returns_team_data(self, crawler):
        page = MagicMock()
        page.goto = AsyncMock()
        page.content = AsyncMock(return_value="<html></html>")
        page.keyboard = MagicMock()
        crawler.page = page

        cols = [MagicMock(), MagicMock(), MagicMock(), MagicMock()]
        cols[0].inner_text = AsyncMock(return_value="LG Twins")
        cols[1].inner_text = AsyncMock(return_value="1990")
        cols[2].inner_text = AsyncMock(return_value="Seoul")

        nth_loc, _ = self._setup_nth_loc(cols, link_count=0)
        page.locator.return_value.nth.return_value = nth_loc
        page.locator.return_value.all = AsyncMock(return_value=[MagicMock()])

        result = await crawler.crawl()

        assert len(result) == 1
        assert result[0]["name"] == "LG Twins"
        assert result[0]["found_year"] == "1990"
        assert result[0]["city"] == "Seoul"

    @mark.asyncio
    @patch.object(TeamInfoCrawler, "start", new=AsyncMock())
    @patch("src.crawlers.team_info_crawler.throttle.wait", new=AsyncMock())
    async def test_parses_modal_when_link_exists(self, crawler):
        page = MagicMock()
        page.goto = AsyncMock()
        page.content = AsyncMock(return_value="<html></html>")
        page.keyboard = MagicMock()
        page.keyboard.press = AsyncMock()
        crawler.page = page

        cols = [MagicMock(), MagicMock(), MagicMock(), MagicMock()]
        cols[0].inner_text = AsyncMock(return_value="SSG Landers")
        cols[1].inner_text = AsyncMock(return_value="2000")
        cols[2].inner_text = AsyncMock(return_value="Incheon")

        nth_loc, link = self._setup_nth_loc(cols, link_count=1)
        link.count = AsyncMock(return_value=1)
        link.click = AsyncMock()

        table_loc = MagicMock()
        table_loc.all = AsyncMock(return_value=[MagicMock()])
        table_loc.nth.return_value = nth_loc

        modal = MagicMock()
        modal.wait_for = AsyncMock()
        modal.is_visible = AsyncMock(return_value=False)
        modal.locator.return_value.count = AsyncMock(return_value=1)
        modal.locator.return_value.inner_text = AsyncMock(side_effect=[
            "Owner Kim", "CEO Lee", "Seoul Address", "02-1234", "https://ssg.com",
        ])
        close_btn = MagicMock()
        close_btn.count = AsyncMock(return_value=0)

        page.locator.side_effect = lambda sel: modal if "layerPop" in sel else table_loc

        result = await crawler.crawl()

        assert len(result) == 1
        assert result[0]["owner"] == "Owner Kim"

    @mark.asyncio
    @patch.object(TeamInfoCrawler, "start", new=AsyncMock())
    @patch("src.crawlers.team_info_crawler.throttle.wait", new=AsyncMock())
    async def test_handles_missing_link_gracefully(self, crawler):
        page = MagicMock()
        page.goto = AsyncMock()
        page.content = AsyncMock(return_value="<html></html>")
        page.keyboard = MagicMock()
        crawler.page = page

        cols = [MagicMock(), MagicMock(), MagicMock(), MagicMock()]
        cols[0].inner_text = AsyncMock(return_value="Team X")
        cols[1].inner_text = AsyncMock(return_value="2010")
        cols[2].inner_text = AsyncMock(return_value="City")

        nth_loc, _ = self._setup_nth_loc(cols, link_count=0)
        page.locator.return_value.nth.return_value = nth_loc
        page.locator.return_value.all = AsyncMock(return_value=[MagicMock()])

        result = await crawler.crawl()

        assert len(result) == 1
        assert result[0]["owner"] is None


class TestSave:
    @mark.asyncio
    @patch("src.crawlers.team_info_crawler.SessionLocal")
    @patch("src.crawlers.team_info_crawler.select")
    async def test_updates_existing_franchise(self, mock_select, mock_sl, crawler):
        mock_session = MagicMock()
        mock_sl.return_value.__enter__.return_value = mock_session
        mock_franchise = MagicMock()
        mock_franchise.metadata_json = {"existing": "data"}
        mock_franchise.name = "LG Twins"
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_franchise
        mock_session.execute.return_value = mock_result

        data = [{"name": "LG Twins", "found_year": "1990", "owner": "Kim", "ceo": "Lee",
                 "address": "Seoul", "phone": "02-1234", "homepage": "https://lg.com"}]

        await crawler.save(data)

        assert mock_franchise.metadata_json["owner"] == "Kim"
        assert mock_franchise.web_url == "https://lg.com"
        mock_session.commit.assert_called_once()
