"""Model for stadium operational notices and announcements.

Captures game-day operational announcements from official team websites and
KBO channels: gate changes, rain delays/cancellations, entry restrictions,
special event announcements, and urgent notices.
"""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from .base import Base, TimestampMixin


class StadiumOperationNotice(Base, TimestampMixin):
    """Official stadium/team operational notice for a game day."""

    __tablename__ = "stadium_operation_notices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_code: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("stadium_info.stadium_code", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Canonical stadium code (e.g. JAMSIL)",
    )

    # Notice classification
    notice_type: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        comment=("Notice type: GATE_CHANGE / CANCEL / DELAY / ENTRY_RULE / WEATHER / EVENT / PARKING / GENERAL"),
    )
    title: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
        comment="Notice title",
    )
    content: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Full notice body text",
    )

    # Timing
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
        index=True,
        comment="Original publication datetime",
    )
    game_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
        index=True,
        comment="Game date this notice relates to (if applicable)",
    )

    # Source info
    source_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Source name (e.g. LG트윈스공식, 두산베어스공식, KBO)",
    )
    source_url: Mapped[str | None] = mapped_column(
        String(500),
        nullable=True,
        comment="Original URL of the notice",
    )
    external_id: Mapped[str | None] = mapped_column(
        String(200),
        nullable=True,
        comment="External system ID (e.g. tweet_id, article_id)",
    )

    # Flags
    is_urgent: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="0",
        comment="Urgent/emergency notice flag",
    )
    is_confirmed: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default="1",
        comment="Whether notice is confirmed (vs rumor/preview)",
    )

    # Raw data
    raw_snapshot: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Raw crawled/API response snapshot",
    )

    __table_args__ = (
        UniqueConstraint(
            "stadium_code",
            "source_name",
            "external_id",
            name="uq_notice_external",
        ),
        UniqueConstraint(
            "stadium_code",
            "source_name",
            "title",
            "published_at",
            name="uq_notice_content",
        ),
        Index("idx_son_game_date", "game_date"),
        Index("idx_son_published_at", "published_at"),
        Index("idx_son_notice_type", "notice_type"),
        Index("idx_son_source", "source_name"),
        Index("idx_son_urgent", "is_urgent"),
    )

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return (
            f"<StadiumOperationNotice("
            f"stadium='{self.stadium_code}', "
            f"type='{self.notice_type}', "
            f"title='{self.title[:40]}...', "
            f"urgent={self.is_urgent}"
            f")>"
        )
