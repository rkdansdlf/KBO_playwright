from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class DataSource(Base, TimestampMixin):
    __tablename__ = "data_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_key: Mapped[str] = mapped_column(
        String(100), nullable=False, unique=True, comment="Unique source key (e.g. kbo_schedule, lg_ticket_general)"
    )
    source_type: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        comment="official_kbo / official_team / ticket_platform / stadium / third_party / sns",
    )
    team_id: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="Associated team code")
    stadium_id: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="Associated stadium code")
    target_domain: Mapped[str] = mapped_column(
        String(30), nullable=False, comment="ticket / seat / parking / food / roster / event / broadcast / injury / etc"
    )
    reliability: Mapped[str] = mapped_column(
        String(10), nullable=False, default="medium", comment="high / medium / low"
    )
    parser_name: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Name of parser class/module")
    crawl_frequency: Mapped[str | None] = mapped_column(
        String(30), nullable=True, comment="daily / weekly / seasonal / ondemand / event_driven"
    )
    base_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Base URL of the source")
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="Last successful fetch")
    last_content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="SHA-256 of last content")
    is_active: Mapped[bool] = mapped_column(
        default=True, server_default="1", comment="Whether this source is actively monitored"
    )

    __table_args__ = (
        Index("idx_ds_target_domain", "target_domain"),
        Index("idx_ds_team", "team_id"),
        Index("idx_ds_stadium", "stadium_id"),
    )

    def __repr__(self) -> str:
        return f"<DataSource(key='{self.source_key}', type='{self.source_type}', domain='{self.target_domain}')>"


class RawSourceSnapshot(Base, TimestampMixin):
    __tablename__ = "raw_source_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False, index=True
    )
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="When the data was fetched")
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="SHA-256 of raw content")
    raw_html_or_json_path: Mapped[str | None] = mapped_column(
        String(500), nullable=True, comment="Path to stored raw content file"
    )
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="HTTP status code")
    parse_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", comment="pending / done / failed"
    )
    parser_version: Mapped[str | None] = mapped_column(
        String(30), nullable=True, comment="Parser version used for this snapshot"
    )
    reprocess_status: Mapped[str | None] = mapped_column(
        String(20), nullable=True, default=None, comment="pending / done / failed (for batch reprocessing)"
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Error details if parse failed")

    __table_args__ = (
        UniqueConstraint("data_source_id", "content_hash", name="uq_snapshot_content"),
        Index("idx_rss_fetched_at", "fetched_at"),
        Index("idx_rss_parse_status", "parse_status"),
    )

    def __repr__(self) -> str:
        return f"<RawSourceSnapshot(id={self.id}, source_id={self.data_source_id}, hash='{self.content_hash}')>"
