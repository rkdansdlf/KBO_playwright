from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from pytest import mark

from src.crawlers.naver_relay_crawler import NaverRelayCrawler


@pytest.fixture
def crawler():
    return NaverRelayCrawler()


class TestCrawlGameEvents:
    @mark.asyncio
    async def test_delegates_to_crawl_game_relay(self, crawler):
        crawler.crawl_game_relay = AsyncMock(return_value={"game_id": "20241015LGSS0", "events": []})
        result = await crawler.crawl_game_events("20241015LGSS0")
        assert result == {"game_id": "20241015LGSS0", "events": []}
        crawler.crawl_game_relay.assert_awaited_once_with("20241015LGSS0")

    @mark.asyncio
    async def test_returns_none_when_relay_returns_none(self, crawler):
        crawler.crawl_game_relay = AsyncMock(return_value=None)
        result = await crawler.crawl_game_events("20241015LGSS0")
        assert result is None

    @mark.asyncio
    async def test_passes_through_complex_payload(self, crawler):
        payload = {"game_id": "20241015LGSS0", "events": [{"event_seq": 1}], "raw_pbp_rows": []}
        crawler.crawl_game_relay = AsyncMock(return_value=payload)
        result = await crawler.crawl_game_events("20241015LGSS0")
        assert result["events"][0]["event_seq"] == 1
