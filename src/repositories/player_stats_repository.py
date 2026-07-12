"""Repositories for player-level advanced season statistics (Fielding, Baserunning)."""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.models.player import PlayerSeasonBaserunning, PlayerSeasonFielding
from src.utils.player_season_stat_validation import filter_valid_season_stat_payloads

from .team_stats_repository import BaseStatsUpsertRepository


class PlayerSeasonFieldingRepository(BaseStatsUpsertRepository):
    """upsert logic for player-level fielding aggregates."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        super().__init__(PlayerSeasonFielding, ["player_id", "team_id", "year", "position_id"])
        self.last_filter_counts: Counter = Counter()

    def upsert_many(self, records: list[dict[str, Any]]) -> int:
        """Insert or update many.

        Args:
            records: Records.
            records: Records.
            records: Records.

        Returns:
            Integer result.

        """
        valid_records, filter_counts = filter_valid_season_stat_payloads(
            records,
            stat_type="fielding",
        )
        self.last_filter_counts = filter_counts
        return super().upsert_many(valid_records)


class PlayerSeasonBaserunningRepository(BaseStatsUpsertRepository):
    """upsert logic for player-level baserunning aggregates."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        super().__init__(PlayerSeasonBaserunning, ["player_id", "team_id", "year"])
