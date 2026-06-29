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
        """
        Initialize a new instance.

        Args:
            session: Session.
            session: Session.

        """
        self.session = session

    def save_stadium_info(self, data: dict) -> StadiumInfo:
        """
        Save stadium info.

        Args:
            data: Data.
            data: Data.
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
        """
        Get all.

        Returns:
            List of results.

        """
        stmt = select(StadiumInfo).order_by(StadiumInfo.stadium_code)

        return list(self.session.execute(stmt).scalars().all())

    def get_by_code(self, code: str) -> StadiumInfo | None:
        """
        Get by code.

        Args:
            code: Code.
            code: Code.
            code: Code.

        Returns:
            The result of the operation.

        """
        return self.session.get(StadiumInfo, code)

    def save_regulation(self, data: dict) -> StadiumRegulation:
        """
        Save regulation.

        Args:
            data: Data.
            data: Data.
            data: Data.

        Returns:
            StadiumRegulation instance.

        """
        new_record = StadiumRegulation(**data)

        self.session.add(new_record)
        return new_record

    def get_regulations_by_stadium(self, stadium_code: str) -> list[StadiumRegulation]:
        """
        Get regulations by stadium.

        Args:
            stadium_code: Stadium Code.
            stadium_code: Stadium Code.
            stadium_code: Stadium Code.

        Returns:
            List of results.

        """
        stmt = select(StadiumRegulation).where(StadiumRegulation.stadium_code == stadium_code)

        return list(self.session.execute(stmt).scalars().all())
