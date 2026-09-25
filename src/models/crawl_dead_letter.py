"""Dead letter queue for crawl failures that require replay.

``CrawlDeadLetter`` records a replayable failure tied to a single
:class:`src.models.crawl_execution.CrawlExecutionRun`. It intentionally stores
references (snapshot/evidence/payload_ref) rather than duplicating raw payloads,
and keeps its own retry lifecycle separate from data-quality quarantine.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class DlqStatus(StrEnum):
    """Lifecycle states for a dead letter entry."""

    PENDING = "pending"
    RETRYING = "retrying"
    RESOLVED = "resolved"
    EXHAUSTED = "exhausted"
    IGNORED = "ignored"


class CrawlDeadLetter(Base, TimestampMixin):
    """A replayable crawl failure awaiting retry."""

    __tablename__ = "crawl_dead_letters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dlq_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    original_run_id: Mapped[str] = mapped_column(String(36), nullable=False)
    crawler: Mapped[str] = mapped_column(String(64), nullable=False)
    target_type: Mapped[str] = mapped_column(String(32), nullable=False)
    target_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    game_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    failure_stage: Mapped[str] = mapped_column(String(16), nullable=False)
    error_code: Mapped[str] = mapped_column(String(64), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    payload_ref: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    snapshot_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    evidence_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=DlqStatus.PENDING.value)
    replay_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "crawler",
            "target_type",
            "target_id",
            "original_run_id",
            name="uq_crawl_dead_letters_incident",
        ),
        Index("idx_crawl_dead_letters_status_retry", "status", "next_retry_at"),
        Index("idx_crawl_dead_letters_original_run", "original_run_id"),
        Index("idx_crawl_dead_letters_crawler_status", "crawler", "status"),
        Index("idx_crawl_dead_letters_error_code", "error_code"),
        Index("idx_crawl_dead_letters_next_retry", "next_retry_at"),
    )
