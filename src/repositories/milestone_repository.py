"""Repository for Player Milestone entries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import false, select

from src.models.player_milestone import PlayerMilestone

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
            existing.current_val = data.get("current_val", existing.current_val)
            existing.target_val = data.get("target_val", existing.target_val)
            existing.remaining_val = data.get("remaining_val", existing.remaining_val)
            existing.is_achieved = data.get("is_achieved", existing.is_achieved)
            return existing

        record = PlayerMilestone(
            season=season,
            player_id=player_id,
            player_name=data["player_name"],
            team_code=data.get("team_code"),
            milestone_category=category,
            current_val=data.get("current_val", 0),
            target_val=data.get("target_val", 0),
            remaining_val=data.get("remaining_val", 0),
            is_achieved=data.get("is_achieved", False),
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
            .where(PlayerMilestone.season == season, PlayerMilestone.is_achieved == false())
            .order_by(PlayerMilestone.remaining_val.asc())
        )
        return list(self.session.execute(stmt).scalars().all())
