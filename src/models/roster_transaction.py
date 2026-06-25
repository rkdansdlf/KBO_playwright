"""데이터 모델: roster transaction."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class RosterTransaction(Base, TimestampMixin):
    __tablename__ = "roster_transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False, index=True, comment="Date of roster change")
    team_id: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Team code")
    player_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    player_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Player name")
    action: Mapped[str] = mapped_column(String(20), nullable=False, comment="registered / deregistered")
    roster_level: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="first_team",
        comment="first_team / second_team / reserve",
    )
    inferred_to_level: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment="second_team / unknown (inferred destination)",
    )
    source_type: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        default="kbo_today_page",
        comment="kbo_today_page / snapshot_diff / manual / player_movement",
    )
    source_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("data_sources.id", ondelete="SET NULL"),
        nullable=True,
        comment="DataSource ID",
    )
    before_snapshot_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("raw_source_snapshots.id", ondelete="SET NULL"),
        nullable=True,
    )
    after_snapshot_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("raw_source_snapshots.id", ondelete="SET NULL"),
        nullable=True,
    )
    confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="high", comment="high / medium / low")
    dedupe_key: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        comment="Dedup key: date+team+player+action",
    )

    __table_args__ = (
        Index("idx_rt_date_team", "transaction_date", "team_id"),
        Index("idx_rt_action", "action"),
        Index("idx_rt_player", "player_id"),
    )

    def __repr__(self) -> str:
        return f"<RosterTransaction(date={self.transaction_date}, team='{self.team_id}', player='{self.player_name}', action='{self.action}')>"
