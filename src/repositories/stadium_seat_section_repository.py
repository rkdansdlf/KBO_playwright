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
            self._apply_updates(existing, data)
            return existing

        # Use PostgreSQL ON CONFLICT for atomic upsert on (stadium_id, section_name)
        # This handles concurrent inserts and unique constraint violations
        if self.session.bind.dialect.name == "postgresql":
            stmt = pg_insert(StadiumSeatSection).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=["stadium_id", "section_name"],
                set_={k: v for k, v in data.items() if k not in ("stadium_id", "section_name")},
            ).returning(StadiumSeatSection)
            return self.session.execute(stmt).scalar_one()

        # Fallback for other dialects (SQLite, Oracle)
        return self._insert_with_retry(data, stadium_id, name)

    @staticmethod
    def _apply_updates(existing: StadiumSeatSection, data: dict) -> None:
        """Copy the mutable payload fields onto a row that already exists.

        Args:
            existing: The row already stored for this section.
            data: Section data dict.

        """
        for key, value in data.items():
            if key not in ("stadium_id", "section_code", "section_name") and value is not None:
                setattr(existing, key, value)

    def _insert_with_retry(self, data: dict, stadium_id: str, name: str) -> StadiumSeatSection:
        """Insert a section, recovering from a concurrent insert.

        A flush that trips the unique constraint means another writer got there
        first. The rollback clears the poisoned transaction, so the row has to
        be looked up again rather than reused.

        Args:
            data: Section data dict.
            stadium_id: Stadium the section belongs to.
            name: Section name, the fallback lookup key.

        Returns:
            The inserted row, or the row a concurrent writer inserted.

        Raises:
            Exception: Whatever the flush raised, when the row cannot be found.

        """
        new_record = StadiumSeatSection(**data)
        self.session.add(new_record)
        try:
            self.session.flush()
        except Exception:
            self.session.rollback()
            # Retry lookup after rollback
            stmt = select(StadiumSeatSection).where(
                StadiumSeatSection.stadium_id == stadium_id,
                StadiumSeatSection.section_name == name,
            )
            existing = self.session.execute(stmt).scalar_one_or_none()
            if existing:
                self._apply_updates(existing, data)
                return existing
            raise
        else:
            return new_record

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
