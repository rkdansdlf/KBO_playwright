"""
Repository for TicketPrice operations.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models.ticket_price import TicketPrice


class TicketPriceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> TicketPrice:
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
        stmt = (
            select(TicketPrice)
            .where(TicketPrice.team_id == team_id, TicketPrice.season == season)
            .order_by(TicketPrice.seat_grade, TicketPrice.day_type)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_stadium_season(self, stadium_id: str, season: int) -> list[TicketPrice]:
        stmt = (
            select(TicketPrice)
            .where(TicketPrice.stadium_id == stadium_id, TicketPrice.season == season)
            .order_by(TicketPrice.team_id)
        )
        return list(self.session.execute(stmt).scalars().all())

    def bulk_save(self, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.save(data)
            count += 1
        return count
