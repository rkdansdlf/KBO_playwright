"""Repository for Player Milestone entries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.player_milestone import PlayerMilestone
from src.services.milestone_targets import initial_milestone_target

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class MilestoneRepository:
    """Repository for managing player milestone records."""

    def __init__(self, session: Session) -> None:
        """Initialize repository with DB session.

        Args:
            session: DB Session.

        """
        self.session = session

    def save_milestone(self, data: dict[str, Any]) -> PlayerMilestone:
        """Save or update a player milestone record.

        Args:
            data: Milestone dictionary data.

        Returns:
            Saved ORM instance.

        """
        season = data["season"]
        player_id = data["player_id"]
        category = data["milestone_category"]

        stmt = select(PlayerMilestone).where(
            PlayerMilestone.season == season,
            PlayerMilestone.player_id == player_id,
            PlayerMilestone.milestone_category == category,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            if "current_val" in data and data["current_val"] is not None:
                existing.current_val = data["current_val"]
            if data.get("player_name"):
                existing.player_name = data["player_name"]
            if "team_code" in data and data["team_code"] is not None:
                existing.team_code = data["team_code"]
            existing.remaining_val = max(0, existing.target_val - existing.current_val)
            return existing

        current_val = data.get("current_val", 0)
        raw_target = data.get("target_val")
        if isinstance(raw_target, int) and raw_target > 0 and not isinstance(raw_target, bool):
            target_val = raw_target
        else:
            target_val = initial_milestone_target(category, current_val)

        remaining_val = max(0, target_val - current_val)

        record = PlayerMilestone(
            season=season,
            player_id=player_id,
            player_name=data["player_name"],
            team_code=data.get("team_code"),
            milestone_category=category,
            current_val=current_val,
            target_val=target_val,
            remaining_val=remaining_val,
            is_achieved=0,
            achieved_date=data.get("achieved_date"),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_upcoming_milestones(self, season: int = 2026) -> list[PlayerMilestone]:
        """Get unachieved upcoming milestones.

        Args:
            season: Season year.

        Returns:
            List of milestone records sorted by remaining amount.

        """
        stmt = (
            select(PlayerMilestone)
            .where(PlayerMilestone.season == season, PlayerMilestone.is_achieved == 0)
            .order_by(PlayerMilestone.remaining_val.asc())
        )
        return list(self.session.execute(stmt).scalars().all())
