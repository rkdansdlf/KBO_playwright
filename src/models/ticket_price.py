"""데이터 모델: ticket price."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class TicketPrice(Base, TimestampMixin):
    """TicketPrice class."""

    __tablename__ = "ticket_prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Team code")
    stadium_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Stadium code")
    season: Mapped[int] = mapped_column(Integer, nullable=False, comment="Season year")
    seat_grade: Mapped[str] = mapped_column(String(50), nullable=False, comment="Seat grade name (e.g. 블루석)")
    day_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="weekday",
        comment="weekday / weekend / holiday / special",
    )
    audience_type: Mapped[str | None] = mapped_column(
        String(30),
        nullable=True,
        comment="general / adult_member / child_member / youth / military / disabled",
    )
    price: Mapped[int] = mapped_column(Integer, nullable=False, comment="Price in KRW")
    currency: Mapped[str] = mapped_column(String(10), nullable=False, default="KRW")
    source_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("data_sources.id", ondelete="SET NULL"),
        nullable=True,
        comment="DataSource ID",
    )
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Source URL")
    effective_from: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Effective start date")
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Effective end date")

    __table_args__ = (
        UniqueConstraint("team_id", "season", "seat_grade", "day_type", "audience_type", name="uq_ticket_price"),
    )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<TicketPrice(team='{self.team_id}', grade='{self.seat_grade}', price={self.price})>"
