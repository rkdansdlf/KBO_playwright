"""
ORM model for normalized stat rankings.
"""
from __future__ import annotations

from typing import Optional, Dict, Any
from sqlalchemy import Integer, String, Float, Boolean, UniqueConstraint, JSON
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class StatRanking(Base, TimestampMixin):
    """Unified ranking table for multiple stat categories."""

    __tablename__ = "stat_rankings"
    __table_args__ = (
        UniqueConstraint(
            "season",
            "metric",
            "entity_id",
            "entity_type",
            name="uq_stat_rank_metric_entity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    metric: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_label: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(16), nullable=False, default="PLAYER")
    team_id: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    is_tie: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    extra: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    def __repr__(self) -> str:
        return f"<StatRanking(season={self.season}, metric='{self.metric}', entity_id='{self.entity_id}', rank={self.rank})>"
