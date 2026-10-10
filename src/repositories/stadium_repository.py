"""Stadium master repository."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.stadium import Stadium

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class StadiumRepository:
    """StadiumRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.

        """
        self.session = session

    def save(self, data: dict) -> Stadium:
        """Upsert one stadium row by stadium_id.

        Args:
            data: Data.

        Returns:
            Stadium instance.

        """
        stadium_id = data["stadium_id"]
        existing = self.session.get(Stadium, stadium_id)
        if existing:
            for key, value in data.items():
                if value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = Stadium(**data)
        self.session.add(new_record)
        return new_record

    def get_all(self) -> list[Stadium]:
        """Return all stadiums ordered by id.

        Returns:
            List of results.

        """
        stmt = select(Stadium).order_by(Stadium.stadium_id)
        return list(self.session.execute(stmt).scalars().all())

    def get_by_id(self, stadium_id: str) -> Stadium | None:
        """Return one stadium by id.

        Args:
            stadium_id: Stadium id.

        Returns:
            The result of the operation.

        """
        return self.session.get(Stadium, stadium_id)

    def update_park_factor(self, stadium_id: str, homerun_park_factor: float) -> Stadium | None:
        """Update only the HR park factor for one stadium.

        Args:
            stadium_id: Stadium id.
            homerun_park_factor: Homerun park factor.

        Returns:
            The result of the operation.

        """
        existing = self.session.get(Stadium, stadium_id)
        if existing is None:
            return None
        existing.homerun_park_factor = homerun_park_factor
        return existing
