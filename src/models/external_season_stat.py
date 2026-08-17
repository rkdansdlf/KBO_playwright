"""ORM model for immutable provider-specific season statistics."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class ExternalSeasonStat(Base, TimestampMixin):
    """Store a provider's season row without replacing official KBO stats."""

    __tablename__ = "external_season_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_record_key: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        unique=True,
        comment="Stable hash of provider/stat type/season/player/team identity",
    )
    provider: Mapped[str] = mapped_column(String(32), nullable=False, comment="statiz or fangraphs")
    source_key: Mapped[str] = mapped_column(String(100), nullable=False, comment="DataSource.source_key")
    stat_type: Mapped[str] = mapped_column(String(16), nullable=False, comment="batting or pitching")
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="KBO1")
    external_player_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    player_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        nullable=True,
        comment="Conservative link to the canonical KBO player registry",
    )
    player_name: Mapped[str] = mapped_column(String(100), nullable=False)
    team_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    team_code: Mapped[str | None] = mapped_column(String(10), nullable=True)
    metrics: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    metric_metadata: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)
    source_url: Mapped[str] = mapped_column(String(1000), nullable=False)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    parser_version: Mapped[str] = mapped_column(String(32), nullable=False)
    resolution_status: Mapped[str] = mapped_column(
        String(24),
        nullable=False,
        default="unresolved",
        server_default="unresolved",
        comment="resolved|unresolved_team|unresolved_player|target_missing",
    )
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_external_stats_provider_season", "provider", "season", "stat_type"),
        Index("idx_external_stats_player", "player_id", "season", "stat_type"),
        Index("idx_external_stats_resolution", "resolution_status"),
    )

    def __repr__(self) -> str:
        """Return a concise representation of the stored provider row."""
        return (
            f"<ExternalSeasonStat(provider='{self.provider}', stat_type='{self.stat_type}', "
            f"season={self.season}, player='{self.player_name}')>"
        )
