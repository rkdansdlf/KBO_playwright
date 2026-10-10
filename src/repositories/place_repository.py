"""Place (amenity) repository."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.place import Place

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class PlaceRepository:
    """PlaceRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.

        """
        self.session = session

    def save(self, data: dict) -> Place:
        """Upsert one place by (stadium_id, category, name).

        Args:
            data: Data.

        Returns:
            Place instance.

        """
        stmt = (
            select(Place)
            .where(Place.stadium_id == data["stadium_id"])
            .where(Place.category == data["category"])
            .where(Place.name == data["name"])
        )
        existing = self.session.execute(stmt).scalars().first()
        if existing:
            for key, value in data.items():
                if value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = Place(**data)
        self.session.add(new_record)
        return new_record

    def get_by_stadium(self, stadium_id: str) -> list[Place]:
        """Return all places for one stadium.

        Args:
            stadium_id: Stadium id.

        Returns:
            List of results.

        """
        stmt = select(Place).where(Place.stadium_id == stadium_id).order_by(Place.category, Place.name)
        return list(self.session.execute(stmt).scalars().all())

    def get_all(self) -> list[Place]:
        """Return all places ordered by stadium and name.

        Returns:
            List of results.

        """
        stmt = select(Place).order_by(Place.stadium_id, Place.category, Place.name)
        return list(self.session.execute(stmt).scalars().all())
