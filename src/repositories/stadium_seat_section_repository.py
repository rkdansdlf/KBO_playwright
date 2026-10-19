"""Repository for StadiumSeatSection operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.models.stadium_seat_section import StadiumSeatSection

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class StadiumSeatSectionRepository:
    """StadiumSeatSectionRepository class."""

    def __init__(self, session: Session) -> None:
        """Initialize a new instance.

        Args:
            session: Session.

        """
        self.session = session

    def save(self, data: dict) -> StadiumSeatSection:
        """Upsert a seat section.

        Uses PostgreSQL ON CONFLICT for atomic upsert on the unique constraint
        (stadium_id, section_name). Falls back to ORM logic for other dialects.

        Args:
            data: Section data dict.

        Returns:
            StadiumSeatSection instance.

        """
        stadium_id = data["stadium_id"]
        name = data.get("section_name", "")
        code = data.get("section_code")

        # Fast path: check if already exists in this session
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

        # Use PostgreSQL ON CONFLICT for atomic upsert on (stadium_id, section_name)
        # This handles concurrent inserts and unique constraint violations
        if self.session.bind.dialect.name == "postgresql":
            stmt = pg_insert(StadiumSeatSection).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=["stadium_id", "section_name"],
                set_={
                    k: v for k, v in data.items()
                    if k not in ("stadium_id", "section_name")
                },
            ).returning(StadiumSeatSection)
            result = self.session.execute(stmt).scalar_one()
            return result

        # Fallback for other dialects (SQLite, Oracle)
        new_record = StadiumSeatSection(**data)
        self.session.add(new_record)
        try:
            self.session.flush()
            return new_record
        except Exception:
            self.session.rollback()
            # Retry lookup after rollback
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
            raise

    def get_by_stadium(self, stadium_id: str) -> list[StadiumSeatSection]:
        """Get by stadium.

        Args:
            stadium_id: Stadium ID.
            stadium_id: Stadium ID.
            stadium_id: Stadium ID.

        Returns:
            List of results.

        """
        stmt = (
            select(StadiumSeatSection)
            .where(StadiumSeatSection.stadium_id == stadium_id)
            .order_by(StadiumSeatSection.section_name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_cheering_sections(self, stadium_id: str) -> list[StadiumSeatSection]:
        """Get cheering sections.

        Args:
            stadium_id: Stadium ID.
            stadium_id: Stadium ID.
            stadium_id: Stadium ID.

        Returns:
            List of results.

        """
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
