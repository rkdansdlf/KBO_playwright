"""Backward-compatible Naver relay crawler shim."""
from __future__ import annotations

from src.crawlers.relay_crawler import RelayCrawler


class NaverRelayCrawler(RelayCrawler):
    async def crawl_game_events(self, game_id: str):
        return await self.crawl_game_relay(game_id)
