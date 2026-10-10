"""Data model: stadiums (KBO stadium master).

Matches the production ``stadiums`` table: stadium id, name, city, home
team, capacity, opened year, left/center fence distance, fence height,
turf, bullpen shape, HR park factor, lat/lng, address, phone.
``homerun_park_factor`` is NULL until the batch calculator
(``ParkFactorCalculator``) fills it from game data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin

if TYPE_CHECKING:
    from .place import Place


class Stadium(Base, TimestampMixin):
    """KBO stadium master row."""

    __tablename__ = "stadiums"

    stadium_id: Mapped[str] = mapped_column(
        String(10),
        primary_key=True,
        comment="Canonical stadium code (e.g. JAMSIL, MUNHAK)",
    )
    stadium_name: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Stadium name in Korean")
    city: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="City (e.g. 서울특별시 송파구)")
    team: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment="Home team code(s), comma-separated for joint use (e.g. LG,OB)",
    )
    capacity: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Seating capacity")
    seating_capacity: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Seating capacity (alternate source)"
    )
    open_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Year opened")
    left_fence_m: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Left-field fence distance (m)")
    center_fence_m: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Center-field fence distance (m)"
    )
    fence_height_m: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Outfield fence height (m)")
    turf_type: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="Turf type (천연/인조)")
    bullpen_type: Mapped[str | None] = mapped_column(
        String(20), nullable=True, comment="Bullpen shape (외야/지하/덕아웃옆)"
    )
    homerun_park_factor: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="HR park factor, 1.00 = neutral (batch-calculated)"
    )
    notes: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Free-form notes")
    lat: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Latitude")
    lng: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Longitude")
    address: Mapped[str | None] = mapped_column(String(300), nullable=True, comment="Full address")
    phone: Mapped[str | None] = mapped_column(String(30), nullable=True, comment="Stadium/team contact phone")

    places: Mapped[list[Place]] = relationship("Place", back_populates="stadium")

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<Stadium(id='{self.stadium_id}', name='{self.stadium_name}')>"
