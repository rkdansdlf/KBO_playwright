"""
Repositories for player-level advanced season statistics (Fielding, Baserunning).
"""
from __future__ import annotations

from typing import List, Dict, Any
from src.models.player import PlayerSeasonFielding, PlayerSeasonBaserunning
from .team_stats_repository import BaseStatsUpsertRepository


class PlayerSeasonFieldingRepository(BaseStatsUpsertRepository):
    """UPSERT logic for player-level fielding aggregates."""

    def __init__(self):
        super().__init__(PlayerSeasonFielding, ["player_id", "team_id", "year", "position_id"])


class PlayerSeasonBaserunningRepository(BaseStatsUpsertRepository):
    """UPSERT logic for player-level baserunning aggregates."""

    def __init__(self):
        super().__init__(PlayerSeasonBaserunning, ["player_id", "team_id", "year"])
