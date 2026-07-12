"""SQLAlchemy model for SLA metrics tracking data freshness."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String

from src.models.base import Base, TimestampMixin


class SlaMetrics(Base, TimestampMixin):
    """Tracks historical SLA checks and data freshness delays."""

    __tablename__ = "sla_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    check_time = Column(DateTime, nullable=False)
    category = Column(String(64), nullable=False)  # "game", "standings", "defense", "relay"
    sla_threshold_hours = Column(Integer, nullable=False)
    actual_delay_hours = Column(Float, nullable=False)
    is_violation = Column(Boolean, nullable=False, default=False)
    notes = Column(String(500), nullable=True)
