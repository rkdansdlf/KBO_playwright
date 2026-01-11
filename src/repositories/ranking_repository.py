"""
Repository for stat rankings.
"""
from __future__ import annotations

from typing import List, Dict, Any

from src.models.rankings import StatRanking
from src.repositories.team_stats_repository import BaseStatsUpsertRepository


class RankingRepository(BaseStatsUpsertRepository):
    """UPSERT interface for stat_rankings."""

    def __init__(self):
        super().__init__(StatRanking, ["season", "metric", "entity_id", "entity_type"])

    def save_rankings(self, rankings: List[Dict[str, Any]]) -> int:
        return self.upsert_many(rankings)
