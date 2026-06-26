"""Backward-compatible Naver relay crawler shim."""

from __future__ import annotations

from typing import Any

from src.crawlers.relay_crawler import RelayCrawler


class NaverRelayCrawler(RelayCrawler):
    """NaverRelayCrawler class."""

    async def crawl_game_events(self, game_id: str) -> dict[str, Any] | None:
        """
        Crawls game events.

        Args:
            game_id: Game ID.

        Returns:
            The result of the operation.

        """
        return await self.crawl_game_relay(game_id)
