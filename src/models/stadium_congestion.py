"""
Model for stadium area congestion measurements.

Captures real-time congestion levels at stadium gates, surrounding subway stations,
roads, and parking areas on game days. Sourced from Seoul Open Data API,
Kakao/Naver map services, and S-DoT sensor data.
"""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Date, DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from .base import Base, TimestampMixin


class StadiumCongestion(Base, TimestampMixin):
    """Real-time congestion measurement at a stadium location point."""

    __tablename__ = "stadium_congestion"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_code: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("stadium_info.stadium_code", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Canonical stadium code (e.g. JAMSIL)",
    )

    # Location
    location_type: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        comment="Location type: gate / subway_station / road / parking / area",
    )
    location_label: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Location label (e.g. 1번게이트, 잠실역_2호선, 올림픽로)",
    )

    # Measurement
    measured_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="Datetime of congestion measurement",
    )
    game_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
        comment="Game date this measurement relates to",
    )

    # Congestion data
    congestion_level: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="Congestion level: low / normal / high / very_high",
    )
    congestion_index: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Numeric congestion index 0~100",
    )
    people_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Estimated crowd count (if available)",
    )

    # Source
    source: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Data source: seoul_open_api / sdot / kakao / naver / manual",
    )
    raw_data: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Raw API/scrape response for audit",
    )

    __table_args__ = (
        UniqueConstraint(
            "stadium_code",
            "location_label",
            "measured_at",
            name="uq_congestion_measurement",
        ),
        Index("idx_sc_game_date", "game_date"),
        Index("idx_sc_measured_at", "measured_at"),
        Index("idx_sc_location_type", "location_type"),
        Index("idx_sc_level", "congestion_level"),
    )

    def __repr__(self) -> str:
        return (
            f"<StadiumCongestion("
            f"stadium='{self.stadium_code}', "
            f"location='{self.location_label}', "
            f"level='{self.congestion_level}', "
            f"index={self.congestion_index}, "
            f"at={self.measured_at}"
            f")>"
        )
