"""Unit tests for BaseCrawler execution lifecycle and stats tracking."""

from __future__ import annotations

import pytest

from src.crawlers.base import BaseCrawler


@pytest.mark.asyncio
async def test_base_crawler_stats_and_throttling() -> None:
    crawler = BaseCrawler(request_delay=0.01)
    stats = crawler.get_stats()
    assert stats.requests_count == 0

    await crawler.throttle()
    assert stats.throttled_seconds >= 0.0

    stats.record_request(success=True)
    assert stats.requests_count == 1
    assert stats.success_count == 1
