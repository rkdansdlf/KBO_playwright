"""데이터 모델: foreign player."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import JSON, Date, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class ForeignPlayerChange(Base, TimestampMixin):
    """ForeignPlayerChange class."""

    __tablename__ = "foreign_player_changes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        nullable=True,
    )
    player_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Foreign player name")
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, comment="Team code")
    season: Mapped[int] = mapped_column(Integer, nullable=False, comment="Season year")
    change_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="SIGNED / RELEASED / REPLACED / RENEWED",
    )
    previous_team: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Previous team/league")
    replacement_reason: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="INJURY / PERFORMANCE / ETC",
    )
    announcement_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Announcement date")
    contract_amount: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Contract amount text")
    stats_before_change: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Stats before replacement",
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Additional notes")
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Source URL")

    __table_args__ = (
        UniqueConstraint("player_name", "team_id", "season", "change_type", name="uq_foreign_player_change"),
        Index("idx_fp_team_season", "team_id", "season"),
        Index("idx_fp_player", "player_id"),
    )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return (
            f"<ForeignPlayerChange(player='{self.player_name}', team='{self.team_id}', "
            f"type='{self.change_type}', season={self.season})>"
        )
