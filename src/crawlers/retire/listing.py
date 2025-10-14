"""
Determine retired/inactive player IDs by comparing historical rosters with current active rosters.
"""
from __future__ import annotations

import asyncio
from typing import Iterable, List, Set, Dict

from src.crawlers.player_list_crawler import PlayerListCrawler


class RetiredPlayerListingCrawler:
    """
    Fetch player ID sets for historical seasons and compute inactive (retired) candidates.
    """

    def __init__(self, request_delay: float = 1.5):
        self.player_list_crawler = PlayerListCrawler(request_delay=request_delay)

    async def collect_player_ids_for_year(self, season_year: int) -> Set[str]:
        """Collect all player IDs (hitters + pitchers) for a given season."""
        results = await self.player_list_crawler.crawl_all_players(season_year=season_year)
        return self._extract_ids(results)

    async def collect_historical_player_ids(self, seasons: Iterable[int]) -> Set[str]:
        historical_ids: Set[str] = set()
        for season in seasons:
            season_ids = await self.collect_player_ids_for_year(season)
            historical_ids |= season_ids
        return historical_ids

    async def determine_inactive_player_ids(
        self,
        start_year: int,
        end_year: int,
        active_year: int,
    ) -> Set[str]:
        """
        Determine inactive player IDs by diffing historical seasons with active roster.
        """
        if start_year > end_year:
            raise ValueError("start_year must be <= end_year")

        seasons = range(start_year, end_year + 1)
        historical_ids = await self.collect_historical_player_ids(seasons)
        active_ids = await self.collect_player_ids_for_year(active_year)
        return {pid for pid in historical_ids if pid and pid not in active_ids}

    def _extract_ids(self, data: Dict[str, List[Dict]]) -> Set[str]:
        ids: Set[str] = set()
        for key in ("hitters", "pitchers"):
            for player in data.get(key, []):
                player_id = player.get("player_id")
                if player_id:
                    ids.add(player_id)
        return ids


async def main():
    crawler = RetiredPlayerListingCrawler(request_delay=1.0)
    inactive_ids = await crawler.determine_inactive_player_ids(
        start_year=1982, end_year=2023, active_year=2024
    )
    print(f"Inactive player IDs discovered: {len(inactive_ids)}")


if __name__ == "__main__":
    asyncio.run(main())

