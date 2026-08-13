"""Tests for newly added KBO crawlers."""

from __future__ import annotations

import pytest

from src.crawlers.futures_schedule_crawler import FuturesScheduleCrawler
from src.crawlers.milestone_crawler import MilestoneCrawler
from src.crawlers.press_release_crawler import PressReleaseCrawler


@pytest.mark.asyncio
async def test_press_release_crawler_instantiation() -> None:
    """Test PressReleaseCrawler instantiation and mock response."""
    crawler = PressReleaseCrawler()
    assert crawler.PRESS_URL == "https://www.koreabaseball.com/MediaNews/Notice/List.aspx"


@pytest.mark.asyncio
async def test_milestone_crawler_instantiation() -> None:
    """Test MilestoneCrawler instantiation."""
    crawler = MilestoneCrawler()
    assert "Hitter.aspx" in crawler.HIT_MILESTONE_URL
    assert "Pitcher.aspx" in crawler.PIT_MILESTONE_URL


@pytest.mark.asyncio
async def test_futures_schedule_crawler_instantiation() -> None:
    """Test FuturesScheduleCrawler instantiation."""
    crawler = FuturesScheduleCrawler()
    assert crawler.SCHEDULE_URL == "https://www.koreabaseball.com/Futures/Schedule/GameList.aspx"
