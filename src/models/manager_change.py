"""데이터 모델: manager change."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class ManagerChange(Base, TimestampMixin):
    """ManagerChange class."""

    __tablename__ = "manager_changes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Team code")
    season: Mapped[int] = mapped_column(Integer, nullable=False, comment="Season year")
    previous_manager: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Previous manager name")
    new_manager: Mapped[str] = mapped_column(String(100), nullable=False, comment="New manager name")
    change_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Date of change")
    change_reason: Mapped[str | None] = mapped_column(
        String(30),
        nullable=True,
        comment="RESIGN / FIRED / INTERIM / CONTRACT_END / PROMOTION",
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Additional notes")
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Source URL")

    __table_args__ = (
        UniqueConstraint("team_id", "season", "new_manager", name="uq_manager_change"),
        Index("idx_mgr_team_season", "team_id", "season"),
    )

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<ManagerChange(team='{self.team_id}', new='{self.new_manager}', season={self.season})>"
