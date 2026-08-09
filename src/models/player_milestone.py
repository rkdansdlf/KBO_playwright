"""ORM model for player upcoming milestones."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class PlayerMilestone(Base, TimestampMixin):
    """Player Milestone Entry (대기록 달성 임박 및 달성 현황)."""

    __tablename__ = "player_milestones"

    __table_args__ = (
        UniqueConstraint("season", "player_id", "milestone_category", name="uq_player_milestone"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    player_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    player_name: Mapped[str] = mapped_column(String(50), nullable=False)
    team_code: Mapped[str | None] = mapped_column(String(20), nullable=True)

    milestone_category: Mapped[str] = mapped_column(String(50), nullable=False, comment="2000안타, 100승, 300세이브 등")
    current_val: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    target_val: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    remaining_val: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    is_achieved: Mapped[bool] = mapped_column(Integer, nullable=False, default=False)
    achieved_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    def __repr__(self) -> str:
        """Return representation string."""
        return (
            f"<PlayerMilestone(player={self.player_name}, category={self.milestone_category}, "
            f"progress={self.current_val}/{self.target_val}, remaining={self.remaining_val})>"
        )
