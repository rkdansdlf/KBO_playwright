"""데이터 모델: crawl."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class CrawlRun(Base):
    """CrawlRun class."""

    __tablename__ = "crawl_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    active_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retired_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    staff_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confirmed_profiles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    heuristic_only: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(UTC).replace(tzinfo=None),
    )
