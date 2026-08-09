"""Immutable evidence linking source captures to persisted data."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class CrawlEvidence(Base, TimestampMixin):
    """Store source artifacts and hashes for one crawl result."""

    __tablename__ = "crawl_evidence"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(128), nullable=False)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    source_name: Mapped[str] = mapped_column(String(128), nullable=False)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    raw_artifact_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    parsed_payload_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    normalized_payload_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    raw_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parsed_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    normalized_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    db_projection_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parser_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    normalization_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    validation_status: Mapped[str] = mapped_column(String(32), nullable=False, default="captured")
    diff_summary: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    capture_metadata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_crawl_evidence_entity", "entity_type", "entity_id", "dataset"),
        Index("idx_crawl_evidence_status", "validation_status"),
        Index("idx_crawl_evidence_raw_hash", "raw_hash"),
    )
