"""Generic crawl execution run ledger.

Unlike :class:`src.models.crawl.CrawlRun` (a player-profile aggregate ledger),
this model records one execution of a crawler against a single target so that
partial failures, retries, DLQ entries, and snapshot replays can all reference a
stable ``run_id``.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin

RUN_STATUS_RUNNING = "running"
RUN_STATUS_SUCCESS = "success"
RUN_STATUS_PARTIAL = "partial"
RUN_STATUS_FAILED = "failed"
RUN_STATUSES = frozenset(
    {
        RUN_STATUS_RUNNING,
        RUN_STATUS_SUCCESS,
        RUN_STATUS_PARTIAL,
        RUN_STATUS_FAILED,
    },
)


class CrawlExecutionRun(Base, TimestampMixin):
    """Record one crawler execution for a single target."""

    __tablename__ = "crawl_execution_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    crawler: Mapped[str] = mapped_column(String(64), nullable=False)
    target_type: Mapped[str] = mapped_column(String(32), nullable=False)
    target_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    season: Mapped[int | None] = mapped_column(Integer, nullable=True)
    game_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=RUN_STATUS_RUNNING)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    records_read: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    records_written: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    checkpoint: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    parser_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    snapshot_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    evidence_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    parent_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    replay_of_run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    __table_args__ = (
        Index("idx_crawl_execution_runs_crawler", "crawler", "started_at"),
        Index("idx_crawl_execution_runs_status", "status"),
        Index("idx_crawl_execution_runs_game", "game_id"),
        Index("idx_crawl_execution_runs_parent", "parent_run_id"),
        Index("idx_crawl_execution_runs_replay_of", "replay_of_run_id"),
    )
