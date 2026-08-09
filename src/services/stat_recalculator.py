"""Service for recalculating player milestones and situational splits."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.game import GameBattingStat
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class StatRecalculator:
    """Service class for updating player milestones and situational splits."""

    def __init__(self, session: Session) -> None:
        """Initialize recalculator with DB session."""
        self.session = session

    def recalc_player_milestones(self, season: int = 2026) -> int:
        """Recalculate remaining milestones for active players based on game stats.

        Args:
            season: Target season.

        Returns:
            Number of milestones updated.

        """
        stmt = select(PlayerMilestone).where(PlayerMilestone.season == season)
        milestones = list(self.session.execute(stmt).scalars().all())
        updated_count = 0

        for m in milestones:
            if not m.player_id:
                continue

            # Query season hits/wins depending on milestone category
            if "안타" in m.milestone_category:
                stat_stmt = select(GameBattingStat.hits).where(
                    GameBattingStat.player_id == m.player_id
                )
                hits_list = list(self.session.execute(stat_stmt).scalars().all())
                added_hits = sum(h for h in hits_list if h)
                if added_hits > 0:
                    new_val = m.current_val + added_hits
                    m.remaining_val = max(0, m.target_val - new_val)
                    m.is_achieved = m.remaining_val == 0
                    updated_count += 1

        logger.info("Recalculated %d player milestones for season %d.", updated_count, season)
        return updated_count

    def recalc_player_splits(self, season: int = 2026) -> int:
        """Recalculate situational splits stats for players.

        Args:
            season: Target season.

        Returns:
            Number of splits entries recalculated.

        """
        # Fetch existing splits
        stmt = select(PlayerSplitsStat).where(PlayerSplitsStat.season == season)
        splits = list(self.session.execute(stmt).scalars().all())
        updated_count = 0

        for s in splits:
            if s.ab > 0:
                s.avg = round(s.hits / s.ab, 3)
                s.ops = round((s.hits + s.bb) / (s.ab + s.bb) + (s.hits + s.hr) / s.ab, 3) if (s.ab + s.bb) > 0 else 0.0
                updated_count += 1

        logger.info("Recalculated %d player splits stats for season %d.", updated_count, season)
        return updated_count
