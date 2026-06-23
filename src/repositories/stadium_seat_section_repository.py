"""
Repository for StadiumSeatSection operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from ..models.stadium_seat_section import StadiumSeatSection

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class StadiumSeatSectionRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> StadiumSeatSection:
        stadium_id = data["stadium_id"]
        name = data.get("section_name", "")
        code = data.get("section_code")
        if code:
            stmt = select(StadiumSeatSection).where(
                StadiumSeatSection.stadium_id == stadium_id,
                StadiumSeatSection.section_code == code,
            )
            existing = self.session.execute(stmt).scalar_one_or_none()
        else:
            stmt = select(StadiumSeatSection).where(
                StadiumSeatSection.stadium_id == stadium_id,
                StadiumSeatSection.section_name == name,
            )
            existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("stadium_id", "section_code", "section_name") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = StadiumSeatSection(**data)
        self.session.add(new_record)
        return new_record

    def get_by_stadium(self, stadium_id: str) -> list[StadiumSeatSection]:
        stmt = (
            select(StadiumSeatSection)
            .where(StadiumSeatSection.stadium_id == stadium_id)
            .order_by(StadiumSeatSection.section_name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_cheering_sections(self, stadium_id: str) -> list[StadiumSeatSection]:
        stmt = (
            select(StadiumSeatSection)
            .where(
                StadiumSeatSection.stadium_id == stadium_id,
                (StadiumSeatSection.is_home_cheering) | (StadiumSeatSection.is_away_cheering),
            )
            .order_by(StadiumSeatSection.section_name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def bulk_save(self, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.save(data)
            count += 1
        return count
