"""
Model representing ticket reservation booking times and schedules.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Optional
from sqlalchemy import Integer, String, Date, DateTime, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base, TimestampMixin

class TicketSchedule(Base, TimestampMixin):
    """
    Represents structured game ticketing open schedule.
    """
    __tablename__ = "ticket_schedules"
    __table_args__ = (
        UniqueConstraint("game_date", "home_team", "platform", name="uq_ticket_schedule"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True, comment="Date of the game")
    home_team: Mapped[str] = mapped_column(String(20), nullable=False, comment="Home team code")
    away_team: Mapped[str] = mapped_column(String(20), nullable=False, comment="Away team code")
    stadium: Mapped[str] = mapped_column(String(50), nullable=False, comment="Name of the stadium")
    open_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="Ticketing open date and time")
    platform: Mapped[str] = mapped_column(String(50), nullable=False, comment="Ticketing platform (e.g., Interpark, Ticketlink)")
    url: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, comment="Ticketing link URL")

    def __repr__(self) -> str:
        return f"<TicketSchedule(date={self.game_date}, home={self.home_team}, open_time={self.open_time})>"
