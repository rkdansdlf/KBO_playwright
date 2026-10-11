"""Service for recalculating player milestones and situational splits."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.game import GameBattingStat, GamePitchingStat
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
            added = self._milestone_progress(m.milestone_category, m.player_id)
            if added > 0:
                new_val = m.current_val + added
                m.current_val = new_val
                m.remaining_val = max(0, m.target_val - new_val)
                # NOTE: DDL stores is_achieved as integer; coerce the bool.
                m.is_achieved = int(m.remaining_val == 0)
                updated_count += 1

        logger.info("Recalculated %d player milestones for season %d.", updated_count, season)
        return updated_count

    def _milestone_progress(self, category: str, player_id: str) -> int:
        """Sum game-level stats feeding a milestone category."""
        if "안타" in category:
            return self._sum_column(GameBattingStat, "hits", player_id)
        if "홈런" in category:
            return self._sum_column(GameBattingStat, "home_runs", player_id)
        if "탈삼진" in category:
            return self._sum_column(GamePitchingStat, "strikeouts", player_id)
        if "세이브" in category:
            return self._sum_column(GamePitchingStat, "saves", player_id)
        if "승" in category:
            return self._sum_column(GamePitchingStat, "wins", player_id)
        return 0

    def _sum_column(self, model: object, column: str, player_id: str) -> int:
        """Sum a nullable integer column over a player's game rows."""
        stat_stmt = select(getattr(model, column)).where(model.player_id == player_id)
        values = list(self.session.execute(stat_stmt).scalars().all())
        return sum(v for v in values if v)

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
