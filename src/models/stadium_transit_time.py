"""
Model for measured transit times to stadiums.

Captures real-time travel duration from nearby transit hubs (subway/bus stops)
to the stadium on game days, collected from map APIs (Kakao, Naver, TMAP).

"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from .base import Base, TimestampMixin


class StadiumTransitTime(Base, TimestampMixin):
    """Measured transit time from an origin point to the stadium."""

    __tablename__ = "stadium_transit_times"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_code: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("stadium_info.stadium_code", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Canonical stadium code (e.g. JAMSIL)",
    )

    # Origin point
    origin_label: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        comment="Origin label (e.g. 잠실역_2호선_7번출구)",
    )
    origin_lat: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Origin latitude")
    origin_lng: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Origin longitude")

    # Transport
    transport_mode: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="Transport mode: subway / bus / walk / car / mixed",
    )

    # Measurement
    measured_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        index=True,
        comment="Datetime when transit time was measured",
    )
    game_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
        comment="Game date this measurement relates to",
    )
    duration_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Measured travel duration in minutes",
    )
    distance_meters: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Travel distance in meters",
    )
    congestion_factor: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        comment="Congestion multiplier vs baseline (1.0 = normal)",
    )

    # Source API
    source_api: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        comment="API source: kakao / naver / tmap / google",
    )
    raw_response: Mapped[dict[str, Any] | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Raw API response for audit trail",
    )

    __table_args__ = (
        UniqueConstraint(
            "stadium_code",
            "origin_label",
            "transport_mode",
            "measured_at",
            name="uq_transit_measurement",
        ),
        Index("idx_stt_game_date", "game_date"),
        Index("idx_stt_measured_at", "measured_at"),
        Index("idx_stt_origin", "origin_label"),
        Index("idx_stt_mode", "transport_mode"),
    )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return (
            f"<StadiumTransitTime("
            f"stadium='{self.stadium_code}', "
            f"origin='{self.origin_label}', "
            f"mode='{self.transport_mode}', "
            f"duration={self.duration_minutes}min, "
            f"at={self.measured_at}"
            f")>"
        )
