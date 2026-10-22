"""Service for recalculating player milestones and situational splits."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.constants import KST
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

    def recalc_player_milestones(self, season: int = 2026, today: date | None = None) -> int:
        """Normalise ``remaining_val`` and ``is_achieved`` from the stored ``current_val``/``target_val``.

        ``current_val`` is a snapshot of a player's running total taken from the source, so it is never
        re-derived here. The previous implementation added the sum of **all** ``game_*_stats`` rows to
        ``current_val`` on every run. That is not idempotent and has no season filter, so each nightly run
        inflated the totals (e.g. a 300-save target showing 35,417) and flagged most rows as achieved.

        This method is idempotent: running it twice changes nothing the second time. When a milestone
        flips from not-achieved (0) to achieved (1) and ``achieved_date`` is unset, ``achieved_date`` is
        stamped with ``today``.

        Args:
            season: Target season.
            today: Optional date to stamp when a milestone flips to achieved. Defaults to today in KST.

        Returns:
            Number of milestone rows whose derived fields changed.

        """
        if today is None:
            today = datetime.now(tz=KST).date()

        stmt = select(PlayerMilestone).where(PlayerMilestone.season == season)
        milestones = list(self.session.execute(stmt).scalars().all())
        updated_count = 0

        for m in milestones:
            remaining = max(0, m.target_val - m.current_val)
            # NOTE: DDL stores is_achieved as integer; compare and assign integers, not bools.
            achieved = int(m.target_val > 0 and m.current_val >= m.target_val)
            prev_achieved = int(m.is_achieved)

            changed = False
            if m.remaining_val != remaining:
                m.remaining_val = remaining
                changed = True

            if prev_achieved != achieved:
                m.is_achieved = achieved
                changed = True
                if prev_achieved == 0 and achieved == 1 and m.achieved_date is None:
                    m.achieved_date = today

            if changed:
                updated_count += 1

        logger.info("Normalised %d player milestones for season %d.", updated_count, season)
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
