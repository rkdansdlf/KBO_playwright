"""데이터 모델: injury."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class InjuryEntry(Base, TimestampMixin):
    """InjuryEntry class."""

    __tablename__ = "injury_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    player_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Player name")
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Team code")
    body_part: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Body part injured")
    injury_type: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment="Type of injury (e.g. elbow strain)",
    )
    injury_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Date of injury occurrence")
    il_placement_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Date placed on IL")
    expected_return_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Expected return date")
    actual_return_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Actual return date")
    severity: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment="MINOR / MODERATE / SEVERE / OUT_FOR_SEASON",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="ACTIVE",
        comment="ACTIVE / RETURNED / 15_IL / 60_IL / OUT_FOR_SEASON",
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Additional notes")
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Source URL")

    __table_args__ = (
        UniqueConstraint("player_id", "il_placement_date", name="uq_injury_entry"),
        Index("idx_injury_status", "status"),
        Index("idx_injury_team", "team_id"),
        Index("idx_injury_player", "player_id"),
    )

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<InjuryEntry(player='{self.player_name}', team='{self.team_id}', status='{self.status}')>"
