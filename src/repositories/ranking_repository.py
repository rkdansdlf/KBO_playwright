"""Repository for stat rankings."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from src.models.rankings import StatRanking
from src.repositories.team_stats_repository import BaseStatsUpsertRepository


class RankingRepository(BaseStatsUpsertRepository):
    """upsert interface for stat_rankings."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance."""
        super().__init__(session, StatRanking, ["season", "metric", "entity_id", "entity_type"])

    def save_rankings(self, rankings: list[dict[str, Any]]) -> int:
        """Save rankings.

        Args:
            rankings: Rankings.
            rankings: Rankings.
            rankings: Rankings.

        Returns:
            Integer result.

        """
        return self.upsert_many(rankings)
