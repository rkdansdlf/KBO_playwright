"""ORM model for KBO official press releases and administrative notices."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class KboPressRelease(Base, TimestampMixin):
    """KBO Official Press Release and Notice."""

    __tablename__ = "kbo_press_releases"

    __table_args__ = (UniqueConstraint("notice_id", name="uq_kbo_press_release_notice_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    notice_id: Mapped[str] = mapped_column(String(50), nullable=False)
    published_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    category: Mapped[str] = mapped_column(String(50), nullable=False, default="공지")
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    content_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    source_url: Mapped[str] = mapped_column(String(500), nullable=False)
    attachment_url: Mapped[str | None] = mapped_column(String(500), nullable=True)

    def __repr__(self) -> str:
        """Return representation string."""
        return f"<KboPressRelease(notice_id={self.notice_id}, date={self.published_date}, title={self.title[:20]})>"
