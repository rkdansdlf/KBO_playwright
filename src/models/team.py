"""
Team-related ORM models
"""
from __future__ import annotations

from typing import Optional
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class Team(Base, TimestampMixin):
    """
    Represents a KBO team.
    Data is seeded from Docs/schema/teams (구단 정보).csv
    """
    __tablename__ = "teams"

    team_id: Mapped[str] = mapped_column(String(10), primary_key=True, comment="구단 고유 ID")
    team_name: Mapped[str] = mapped_column(String(50), nullable=False, comment="구단 정식 명칭")
    team_short_name: Mapped[str] = mapped_column(String(20), nullable=False, comment="구단 약칭")
    city: Mapped[str] = mapped_column(String(30), nullable=False, comment="연고 도시")
    founded_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, comment="창단 연도")
    stadium_name: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, comment="홈 구장 명칭")
    franchise_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, comment="Franchise ID")

    # franchise: Mapped["Franchise"] = relationship(back_populates="teams")

    def __repr__(self) -> str:
        return f"<Team(team_id='{self.team_id}', name='{self.team_name}')>"