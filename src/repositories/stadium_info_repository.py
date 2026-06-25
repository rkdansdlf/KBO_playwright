"""stadium info repository 리포지토리."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.stadium_info import StadiumInfo, StadiumRegulation

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class StadiumInfoRepository:
    """StadiumInfoRepository class."""

    def __init__(self, session: Session) -> None:
        """Initializes a new instance."""
        self.session = session

    def save_stadium_info(self, data: dict) -> StadiumInfo:
        """Saves stadium info.

        Args:
            data: Data.

        Returns:
            StadiumInfo instance.

        """
        code = data["stadium_code"]
        existing = self.session.get(StadiumInfo, code)
        if existing:
            for key, value in data.items():
                if value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = StadiumInfo(**data)
        self.session.add(new_record)
        return new_record

    def get_all(self) -> list[StadiumInfo]:
        """Gets all.

        Returns:
            List of results.

        """
        stmt = select(StadiumInfo).order_by(StadiumInfo.stadium_code)
        return list(self.session.execute(stmt).scalars().all())

    def get_by_code(self, code: str) -> StadiumInfo | None:
        """Gets by code.

        Args:
            code: Code.

        Returns:
            The result of the operation.

        """
        return self.session.get(StadiumInfo, code)

    def save_regulation(self, data: dict) -> StadiumRegulation:
        """Saves regulation.

        Args:
            data: Data.

        Returns:
            StadiumRegulation instance.

        """
        new_record = StadiumRegulation(**data)
        self.session.add(new_record)
        return new_record

    def get_regulations_by_stadium(self, stadium_code: str) -> list[StadiumRegulation]:
        """Gets regulations by stadium.

        Args:
            stadium_code: Stadium Code.

        Returns:
            List of results.

        """
        stmt = select(StadiumRegulation).where(StadiumRegulation.stadium_code == stadium_code)
        return list(self.session.execute(stmt).scalars().all())
