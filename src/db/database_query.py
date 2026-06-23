"""
Database Query Service
Provides specialized query functions for player and game data.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import and_, select

from src.db.engine import SessionLocal
from src.models.player import PlayerBasic, PlayerSeasonFielding

logger = logging.getLogger(__name__)


def get_player_defensive_stats(player_name: str, year: int) -> list[dict[str, Any]]:
    """
    Retrieves defensive statistics for a specific player and year.

    Args:
        player_name: The name of the player.
        year: The season year.

    Returns:
        A list of dictionaries containing defensive statistics for each position played.
    """
    with SessionLocal() as session:
        # 1. Resolve player name to player_id
        # We might have multiple players with the same name, so we return stats for all of them
        # or we could filter by active status if needed.
        player_stmt = select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
        player_ids = [row[0] for row in session.execute(player_stmt).all()]

        if not player_ids:
            return []

        # 2. Query fielding stats
        fielding_stmt = select(PlayerSeasonFielding).where(
            and_(PlayerSeasonFielding.player_id.in_(player_ids), PlayerSeasonFielding.year == year),
        )

        return [
            {
                "player_id": row.player_id,
                "team_id": row.team_id,
                "year": row.year,
                "position_id": row.position_id,
                "games": row.games,
                "games_started": row.games_started,
                "innings": row.innings,
                "putouts": row.putouts,
                "assists": row.assists,
                "errors": row.errors,
                "double_plays": row.double_plays,
                "fielding_pct": row.fielding_pct,
                "pickoffs": row.pickoffs,
                "source": row.source,
            }
            for row in session.execute(fielding_stmt).scalars().all()
        ]


if __name__ == "__main__":
    # Test
    stats = get_player_defensive_stats("김현수", 2025)
    for s in stats:
        logger.info(s)
