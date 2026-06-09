from __future__ import annotations

from datetime import time

from sqlalchemy import ForeignKey, Integer, String, Text, Time, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class TicketOpenRule(Base, TimestampMixin):
    __tablename__ = "ticket_open_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Team code")
    platform: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Ticketing platform (Ticketlink / Interpark / self)",
    )
    open_offset_days: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Days before game day (e.g. 7 = N일 전)",
    )
    open_time: Mapped[time] = mapped_column(Time, nullable=False, comment="Opening time on open day (e.g. 11:00)")
    sales_close_rule: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Sales close rule description")
    max_tickets_per_user: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Maximum tickets per person",
    )
    fee_rule: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Ticketing fee description")
    cancel_rule: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Cancellation policy")
    source_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("data_sources.id", ondelete="SET NULL"),
        nullable=True,
        comment="DataSource ID",
    )
    note: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Additional notes")

    __table_args__ = (
        UniqueConstraint("team_id", "platform", "open_offset_days", "open_time", name="uq_ticket_open_rule"),
    )

    def __repr__(self) -> str:
        return f"<TicketOpenRule(team='{self.team_id}', platform='{self.platform}', offset={self.open_offset_days}d, at={self.open_time})>"
