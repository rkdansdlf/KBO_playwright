"""Player projection ORM model for storing forecast metrics with parameter lineage."""

from __future__ import annotations

from sqlalchemy import JSON, Column, Float, Integer, Numeric, String, UniqueConstraint

from src.models.base import Base, TimestampMixin


class PlayerProjection(Base, TimestampMixin):
    """Storage for projected season statistics with full reproducibility lineage."""

    __tablename__ = "player_projections"
    __table_args__ = (
        UniqueConstraint("target_season", "player_id", "position_type", "version", name="uq_player_projection"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    target_season = Column(Integer, nullable=False, index=True)
    player_id = Column(Integer, nullable=False, index=True)
    player_name = Column(String(64), nullable=False)
    team_code = Column(String(10), nullable=True)
    position_type = Column(String(16), nullable=False)  # 'HITTER', 'PITCHER'
    age = Column(Integer, nullable=True)

    # Core forecast metrics
    projected_pa = Column(Float, nullable=True)
    projected_ip = Column(Numeric(6, 2), nullable=True)
    projected_avg = Column(Float, nullable=True)
    projected_obp = Column(Float, nullable=True)
    projected_slg = Column(Float, nullable=True)
    projected_ops = Column(Float, nullable=True)
    projected_woba = Column(Float, nullable=True)
    projected_era = Column(Float, nullable=True)
    projected_fip = Column(Float, nullable=True)
    projected_whip = Column(Float, nullable=True)

    # Full stats dictionary & Lineage
    projected_stats = Column(JSON, nullable=False)
    weights_used = Column(JSON, nullable=False)
    regression_params = Column(JSON, nullable=False)
    version = Column(String(32), nullable=False, default="marcel-v1")


__all__ = ["PlayerProjection"]
