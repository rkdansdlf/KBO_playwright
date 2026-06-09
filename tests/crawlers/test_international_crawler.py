from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark

from src.crawlers.international_crawler import InternationalScheduleCrawler


@pytest.fixture
def crawler():
    return InternationalScheduleCrawler()


def make_locator_mock():
    """Create a MagicMock that simulates a Playwright Locator."""
    return MagicMock()


def make_async_cell(text: str) -> MagicMock:
    cell = make_locator_mock()
    cell.inner_text = AsyncMock(return_value=text)
    return cell


class TestStartBrowser:
    @mark.asyncio
    @patch("src.crawlers.international_crawler.async_playwright")
    async def test_starts_browser_and_page(self, mock_async_pw, crawler):
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_async_pw.return_value.start = AsyncMock(return_value=mock_pw)
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_context.new_page = AsyncMock(return_value=mock_page)

        await crawler.start_browser()

        assert crawler.browser is mock_browser
        assert crawler.page is mock_page
        assert crawler.context is mock_context
        mock_pw.chromium.launch.assert_awaited_once_with(headless=True)


class TestParseRow:
    @pytest.fixture
    def crawler_with_page(self, crawler):
        crawler.page = MagicMock()
        return crawler

    @mark.asyncio
    @patch("src.crawlers.international_crawler.resolve_team_code")
    async def test_parses_valid_row(self, mock_resolve, crawler_with_page):
        mock_resolve.side_effect = lambda name: {"Korea": "KOR", "Japan": "JPN"}.get(name)
        crawler = crawler_with_page
        mock_row = make_locator_mock()
        mock_cols = [
            make_async_cell("11.13(수)"),
            make_async_cell("19:30"),
            make_locator_mock(),
            make_async_cell("Tokyo Dome"),
        ]
        all_mock = AsyncMock(return_value=mock_cols)
        mock_row.locator.return_value.all = all_mock

        match_cell = mock_cols[2]
        count_mock = AsyncMock(return_value=1)
        match_cell.locator.return_value.count = count_mock
        match_cell.locator.return_value.inner_text = AsyncMock(side_effect=["Korea", "Japan", "3 vs 2"])

        result = await crawler._parse_row(mock_row, 2024)

        assert result is not None
        assert result["game_date"].year == 2024
        assert result["game_time"] == "19:30"
        assert result["away_team"] == "KOR"
        assert result["home_team"] == "JPN"

    @mark.asyncio
    async def test_returns_none_for_short_row(self, crawler_with_page):
        crawler = crawler_with_page
        mock_row = make_locator_mock()
        mock_row.locator.return_value.all = AsyncMock(return_value=[MagicMock(), MagicMock()])

        result = await crawler._parse_row(mock_row, 2024)
        assert result is None

    @mark.asyncio
    async def test_returns_none_when_team_structure_missing(self, crawler_with_page):
        crawler = crawler_with_page
        mock_row = make_locator_mock()
        mock_cols = [
            make_async_cell("11.13(수)"),
            make_async_cell("19:30"),
            make_locator_mock(),
        ]
        mock_row.locator.return_value.all = AsyncMock(return_value=mock_cols)
        match_cell = mock_cols[2]
        match_cell.locator.return_value.count = AsyncMock(return_value=0)

        result = await crawler._parse_row(mock_row, 2024)
        assert result is None


class TestCrawlSchedule:
    @mark.asyncio
    @patch.object(InternationalScheduleCrawler, "start_browser", new=AsyncMock())
    @patch.object(InternationalScheduleCrawler, "_parse_row")
    async def test_crawls_schedule_and_parses_rows(self, mock_parse_row, crawler):
        mock_page = MagicMock()
        mock_page.goto = AsyncMock()
        mock_row1 = MagicMock()
        mock_row2 = MagicMock()
        mock_page.locator.return_value.all = AsyncMock(return_value=[mock_row1, mock_row2])
        crawler.page = mock_page
        mock_parse_row.side_effect = [
            {"game_id": "20241113KORJPN0", "game_date": "2024-11-13"},
            None,
        ]

        result = await crawler.crawl_schedule("https://example.com/2024")

        assert len(result) == 1
        assert result[0]["game_id"] == "20241113KORJPN0"
        mock_page.goto.assert_awaited_once()
