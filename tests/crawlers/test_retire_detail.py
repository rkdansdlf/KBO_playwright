from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.retire.detail import RetiredPlayerDetailCrawler


@pytest.mark.asyncio
class TestRetiredPlayerDetailCrawler:
    async def test_extract_profile_text_and_photo_url_use_fallback_selectors(self):
        profile = MagicMock()
        profile.inner_text = AsyncMock(return_value="  은퇴 선수 프로필  ")
        photo = MagicMock()
        photo.get_attribute = AsyncMock(return_value="/person/123.jpg")
        page = AsyncMock()
        page.query_selector.side_effect = [None, profile, photo]
        crawler = RetiredPlayerDetailCrawler()

        text = await crawler._extract_profile_text(page)
        photo_url = await crawler._extract_photo_url(page)

        assert text == "은퇴 선수 프로필"
        assert photo_url == "https://www.koreabaseball.com/person/123.jpg"

    async def test_fetch_page_returns_profile_photo_and_tables(self):
        crawler = RetiredPlayerDetailCrawler(request_delay=0)
        crawler._wait = AsyncMock()
        crawler._extract_profile_text = AsyncMock(return_value="프로필")
        crawler._extract_photo_url = AsyncMock(return_value="https://example.test/photo.jpg")
        crawler._extract_tables = AsyncMock(return_value=[{"headers": ["G"], "rows": [["10"]]}])
        page = AsyncMock()

        with patch("src.crawlers.retire.detail.compliance.is_allowed", new=AsyncMock(return_value=True)):
            payload = await crawler._fetch_page(page, crawler.hitter_url, "123")

        assert payload == {
            "url": f"{crawler.hitter_url}?playerId=123",
            "profile_text": "프로필",
            "photo_url": "https://example.test/photo.jpg",
            "tables": [{"headers": ["G"], "rows": [["10"]]}],
        }
        page.goto.assert_awaited_once()

    async def test_fetch_page_returns_none_when_content_is_empty(self):
        crawler = RetiredPlayerDetailCrawler(request_delay=0)
        crawler._wait = AsyncMock()
        crawler._extract_profile_text = AsyncMock(return_value=None)
        crawler._extract_photo_url = AsyncMock(return_value=None)
        crawler._extract_tables = AsyncMock(return_value=[])
        page = AsyncMock()

        with patch("src.crawlers.retire.detail.compliance.is_allowed", new=AsyncMock(return_value=True)):
            payload = await crawler._fetch_page(page, crawler.pitcher_url, "123")

        assert payload is None

    async def test_fetch_player_combines_hitter_and_pitcher_using_injected_pool(self):
        page = AsyncMock()
        page_context = MagicMock()
        page_context.__aenter__ = AsyncMock(return_value=page)
        page_context.__aexit__ = AsyncMock(return_value=False)
        pool = MagicMock()
        pool.page.return_value = page_context
        crawler = RetiredPlayerDetailCrawler(pool=pool)
        crawler._fetch_page = AsyncMock(side_effect=[{"tables": ["h"]}, {"tables": ["p"]}])

        payload = await crawler.fetch_player("123")

        assert payload == {"player_id": "123", "hitter": {"tables": ["h"]}, "pitcher": {"tables": ["p"]}}
        assert crawler._fetch_page.await_count == 2

    async def test_extract_tables_returns_page_evaluation_payload(self):
        page = AsyncMock()
        page.eval_on_selector_all.return_value = [{"caption": "통산", "headers": ["G"], "rows": [["10"]]}]

        tables = await RetiredPlayerDetailCrawler()._extract_tables(page)

        assert tables == [{"caption": "통산", "headers": ["G"], "rows": [["10"]]}]
        page.eval_on_selector_all.assert_awaited_once()

    async def test_get_pool_creates_and_starts_internal_pool_once(self):
        created_pool = MagicMock()
        created_pool.start = AsyncMock()
        crawler = RetiredPlayerDetailCrawler()

        with patch("src.crawlers.retire.detail.AsyncPlaywrightPool", return_value=created_pool):
            first = await crawler._get_pool()
            second = await crawler._get_pool()

        assert first is created_pool
        assert second is created_pool
        created_pool.start.assert_awaited_once()

    async def test_close_releases_internal_pool_and_clears_reference(self):
        crawler = RetiredPlayerDetailCrawler()
        pool = MagicMock()
        pool.close = AsyncMock()
        crawler._internal_pool = pool

        await crawler.close()

        pool.close.assert_awaited_once()
        assert crawler._internal_pool is None
