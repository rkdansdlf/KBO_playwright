from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class CrawlRun(Base):
    __tablename__ = "crawl_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    active_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retired_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    staff_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confirmed_profiles: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    heuristic_only: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
