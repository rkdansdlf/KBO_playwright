"""Repository for TicketPrice operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.ticket_price import TicketPrice

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class TicketPriceRepository:
    """TicketPriceRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.
            session: Session.

        """
        self.session = session

    def save(self, data: dict) -> TicketPrice:
        """Save save.

        Args:
            data: Data.
            data: Data.
            data: Data.

        Returns:
            TicketPrice instance.

        """
        stmt = select(TicketPrice).where(
            TicketPrice.team_id == data["team_id"],
            TicketPrice.season == data["season"],
            TicketPrice.seat_grade == data["seat_grade"],
            TicketPrice.day_type == data["day_type"],
            TicketPrice.audience_type == data.get("audience_type"),
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("team_id", "season", "seat_grade", "day_type", "audience_type") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = TicketPrice(**data)
        self.session.add(new_record)
        return new_record

    def get_by_team_season(self, team_id: str, season: int) -> list[TicketPrice]:
        """Get by team season.

        Args:
            team_id: Team ID.
            season: Season year.
            team_id: Team ID.
            season: Season year.
            team_id: Team ID.
            season: Season year.

        Returns:
            List of results.

        """
        stmt = (
            select(TicketPrice)
            .where(TicketPrice.team_id == team_id, TicketPrice.season == season)
            .order_by(TicketPrice.seat_grade, TicketPrice.day_type)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_stadium_season(self, stadium_id: str, season: int) -> list[TicketPrice]:
        """Get by stadium season.

        Args:
            stadium_id: Stadium ID.
            season: Season year.
            stadium_id: Stadium ID.
            season: Season year.
            stadium_id: Stadium ID.
            season: Season year.

        Returns:
            List of results.

        """
        stmt = (
            select(TicketPrice)
            .where(TicketPrice.stadium_id == stadium_id, TicketPrice.season == season)
            .order_by(TicketPrice.team_id)
        )
        return list(self.session.execute(stmt).scalars().all())

    def bulk_save(self, records: list[dict]) -> int:
        """Save bulk.

        Args:
            records: Records.
            records: Records.
            records: Records.

        Returns:
            Integer result.

        """
        count = 0

        for data in records:
            self.save(data)
            count += 1
        return count
