"""manager change repository 리포지토리."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.manager_change import ManagerChange

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class ManagerChangeRepository:
    """ManagerChangeRepository class."""

    def __init__(self, session: Session) -> None:
        """Initializes a new instance."""
        self.session = session

    def save_change(self, data: dict) -> ManagerChange:
        """Saves change.

        Args:
            data: Data.

        Returns:
            ManagerChange instance.

        """
        team_id = data["team_id"]
        season = data["season"]
        new_manager = data["new_manager"]

        stmt = select(ManagerChange).where(
            ManagerChange.team_id == team_id,
            ManagerChange.season == season,
            ManagerChange.new_manager == new_manager,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            for key, value in data.items():
                if key not in ("team_id", "season", "new_manager") and value is not None:
                    setattr(existing, key, value)
            return existing

        new_record = ManagerChange(**data)
        self.session.add(new_record)
        return new_record

    def get_by_team(self, team_id: str) -> list[ManagerChange]:
        """Gets by team.

        Args:
            team_id: Team ID.

        Returns:
            List of results.

        """
        stmt = (
            select(ManagerChange)
            .where(ManagerChange.team_id == team_id)
            .order_by(ManagerChange.change_date.desc().nullslast())
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_season(self, season: int) -> list[ManagerChange]:
        """Gets by season.

        Args:
            season: Season year.

        Returns:
            List of results.

        """
        stmt = (
            select(ManagerChange)
            .where(ManagerChange.season == season)
            .order_by(ManagerChange.change_date.desc().nullslast())
        )
        return list(self.session.execute(stmt).scalars().all())
