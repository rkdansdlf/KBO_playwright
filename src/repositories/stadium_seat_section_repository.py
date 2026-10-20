"""Repository for StadiumSeatSection operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.models.stadium_seat_section import StadiumSeatSection

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

#: Dialects that support ``INSERT ... ON CONFLICT`` on the (stadium_id, section_name) unique key.
_UPSERT_INSERT_BY_DIALECT = {"postgresql": pg_insert, "sqlite": sqlite_insert}
_CONFLICT_COLUMNS = ["stadium_id", "section_name"]


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

        Uses ``INSERT ... ON CONFLICT`` (PostgreSQL and SQLite) for an atomic upsert on
        the unique constraint (stadium_id, section_name). Other dialects get a plain insert.
        The repository never commits or rolls back: the caller owns the transaction.

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

        # A concurrent insert or a unique-key hit resolves inside the statement, so this
        # repository never rolls back a transaction the caller owns.
        insert_fn = _UPSERT_INSERT_BY_DIALECT.get(self.session.bind.dialect.name)
        if insert_fn is not None:
            return self._upsert(insert_fn, data)

        # Any other dialect: a plain insert. A unique violation propagates to the caller,
        # who owns the transaction and decides whether to roll it back.
        new_record = StadiumSeatSection(**data)
        self.session.add(new_record)
        self.session.flush()
        return new_record

    def _upsert(self, insert_fn: object, data: dict) -> StadiumSeatSection:
        """Insert a section, or update the row that already holds its (stadium_id, section_name).

        Args:
            insert_fn: The dialect's ``insert`` construct that supports ``ON CONFLICT``.
            data: Section data dict.

        Returns:
            The inserted or already-stored row.

        """
        updates = {k: v for k, v in data.items() if k not in ("stadium_id", "section_name")}
        stmt = insert_fn(StadiumSeatSection).values(**data)  # type: ignore[operator]
        if updates:
            stmt = stmt.on_conflict_do_update(index_elements=_CONFLICT_COLUMNS, set_=updates)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=_CONFLICT_COLUMNS)
        row = self.session.execute(stmt.returning(StadiumSeatSection)).scalar_one_or_none()
        if row is None:
            # ``DO NOTHING`` returns no row when the section was already stored.
            row = self.session.execute(
                select(StadiumSeatSection).where(
                    StadiumSeatSection.stadium_id == data["stadium_id"],
                    StadiumSeatSection.section_name == data.get("section_name", ""),
                ),
            ).scalar_one()
        return row

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
