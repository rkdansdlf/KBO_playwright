from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from playwright.async_api import Error as PlaywrightError

from src.crawlers.futures.profile import FuturesProfileCrawler


@pytest.mark.asyncio
async def test_scrape_profile_returns_none_after_navigation_failure() -> None:
    crawler = FuturesProfileCrawler(request_delay=0)
    crawler._wait = AsyncMock()
    page = MagicMock()
    page.goto = AsyncMock(side_effect=PlaywrightError("navigation failed"))

    with patch("src.crawlers.futures.profile.compliance.is_allowed", new=AsyncMock(return_value=True)):
        result = await crawler._scrape_profile(page, crawler.hitter_profile_url, "123")

    assert result is None


@pytest.mark.asyncio
async def test_scrape_profile_returns_none_when_no_futures_tables_exist() -> None:
    crawler = FuturesProfileCrawler(request_delay=0)
    crawler._wait = AsyncMock()
    crawler._extract_profile_text = AsyncMock(return_value="profile")
    crawler._extract_futures_tables = AsyncMock(return_value=[])
    page = MagicMock()
    page.goto = AsyncMock()

    with patch("src.crawlers.futures.profile.compliance.is_allowed", new=AsyncMock(return_value=True)):
        result = await crawler._scrape_profile(page, crawler.hitter_profile_url, "123")

    assert result is None


@pytest.mark.asyncio
async def test_extract_profile_text_skips_selector_and_inner_text_failures() -> None:
    broken = MagicMock()
    broken.inner_text = AsyncMock(side_effect=PlaywrightError("unavailable"))
    valid = MagicMock()
    valid.inner_text = AsyncMock(return_value=" profile text ")
    page = MagicMock()
    page.query_selector = AsyncMock(side_effect=[PlaywrightError("missing"), broken, valid])

    result = await FuturesProfileCrawler()._extract_profile_text(page)

    assert result == "profile text"


@pytest.mark.asyncio
async def test_extract_futures_tables_requires_existing_table_when_tab_is_unavailable() -> None:
    crawler = FuturesProfileCrawler()
    crawler._click_futures_tab = AsyncMock(return_value=False)
    page = MagicMock()
    page.query_selector = AsyncMock(return_value=None)

    assert await crawler._extract_futures_tables(page) == []
    page.content.assert_not_called()


@pytest.mark.asyncio
async def test_extract_futures_tables_uses_fallback_container() -> None:
    crawler = FuturesProfileCrawler(request_delay=0)
    crawler._click_futures_tab = AsyncMock(return_value=True)
    crawler._wait = AsyncMock()
    page = MagicMock()
    page.content = AsyncMock(
        return_value="""
        <div id="PlayerFuturesStats">
          <table><tr><th>G</th></tr><tr><td>4</td></tr></table>
        </div>
        """,
    )

    tables = await crawler._extract_futures_tables(page)

    assert tables == [{"caption": None, "summary": "", "headers": ["G"], "rows": [["4"]]}]


@pytest.mark.asyncio
async def test_fetch_player_futures_closes_owned_pool() -> None:
    page = MagicMock()
    pool = MagicMock()
    pool.start = AsyncMock()
    pool.acquire = AsyncMock(return_value=page)
    pool.release = AsyncMock()
    pool.close = AsyncMock()
    crawler = FuturesProfileCrawler()
    crawler._scrape_profile = AsyncMock(side_effect=[None, {"profile_text": "pitcher", "tables": []}])

    with patch("src.crawlers.futures.profile.AsyncPlaywrightPool", return_value=pool):
        payload = await crawler.fetch_player_futures("123")

    assert payload == {"player_id": "123", "profile_text": "pitcher", "tables": []}
    pool.release.assert_awaited_once_with(page)
    pool.close.assert_awaited_once()
