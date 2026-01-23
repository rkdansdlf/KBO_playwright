"""
Award model for KBO awards history.
"""
from __future__ import annotations

from typing import Optional
from sqlalchemy import Integer, String, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class Award(Base, TimestampMixin):
    """
    Represents a KBO award win (e.g. MVP, Golden Glove, Rookie of the Year).
    """
    __tablename__ = "awards"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, comment="Award year")
    award_type: Mapped[str] = mapped_column(String(50), nullable=False, comment="Type of award (e.g. MVP, Golden Glove)")
    category: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, comment="Detailed category (e.g. Pitcher, 1B)")
    player_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Winner name")
    team_name: Mapped[str] = mapped_column(String(50), nullable=False, comment="Winner team")

    __table_args__ = (
        UniqueConstraint(
            "year",
            "award_type",
            "category",
            "player_name",
            "team_name",
            name="uq_award_record",
        ),
        Index("idx_award_year", "year"),
        Index("idx_award_type", "award_type"),
        Index("idx_award_player", "player_name"),
    )

    def __repr__(self) -> str:
        return f"<Award(year={self.year}, type='{self.award_type}', category='{self.category}', player='{self.player_name}')>"
