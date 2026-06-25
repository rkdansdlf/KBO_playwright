"""데이터 모델: team event."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class TeamEvent(Base, TimestampMixin):
    """TeamEvent class."""

    __tablename__ = "team_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_scope: Mapped[str] = mapped_column(String(10), nullable=False, default="team", comment="kbo / team / stadium")
    team_id: Mapped[str | None] = mapped_column(String(10), nullable=True, index=True, comment="Associated team code")
    game_id: Mapped[str | None] = mapped_column(
        String(20),
        ForeignKey("game.game_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Associated game ID",
    )
    stadium_id: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="Associated stadium code")
    title: Mapped[str] = mapped_column(String(300), nullable=False, comment="Event title")
    description: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Event description")
    event_type: Mapped[str | None] = mapped_column(
        String(30),
        nullable=True,
        comment="giveaway / signing / first_pitch / promotion / fan_participation / discount / ceremony / festival / notice",
    )
    event_start_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Event start datetime")
    event_end_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Event end datetime")
    apply_start_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Application start")
    apply_end_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Application deadline")
    location_text: Mapped[str | None] = mapped_column(String(200), nullable=True, comment="Event location")
    target_audience: Mapped[str | None] = mapped_column(String(200), nullable=True, comment="Target audience")
    benefit_text: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Benefit/giveaway details")
    image_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Event image URL")
    source_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("data_sources.id", ondelete="SET NULL"),
        nullable=True,
        comment="DataSource ID",
    )
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Original source URL")
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="When published")
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC).replace(tzinfo=None),
        comment="Last time this event was observed",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="unknown",
        comment="scheduled / open / closed / ended / unknown",
    )

    __table_args__ = (
        UniqueConstraint("team_id", "title", "source_url", name="uq_team_event"),
        Index("idx_team_event_scope", "event_scope"),
        Index("idx_team_event_type", "event_type"),
        Index("idx_team_event_status", "status"),
        Index("idx_team_event_published", "published_at"),
    )

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<TeamEvent(id={self.id}, title='{self.title}', scope='{self.event_scope}', status='{self.status}')>"
