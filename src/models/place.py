"""Data model: places (in-stadium amenities).

Matches the production ``places`` table: stadium id, category, name,
description (location inside the stadium), coordinates, contact, rating,
and opening/closing time. Times stay NULL when the source page does not
publish them (common for restrooms). ``lat``/``lng`` are NOT NULL in the
database, so amenity rows fall back to the stadium coordinates.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Float, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin

if TYPE_CHECKING:
    from .stadium import Stadium


class Place(Base, TimestampMixin):
    """A single amenity inside a stadium."""

    __tablename__ = "places"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_id: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("stadiums.stadium_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to stadiums.stadium_id",
    )
    category: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        comment="Amenity category (음식점/화장실/매점/용품점/기타)",
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Amenity name")
    description: Mapped[str | None] = mapped_column(
        String(500), nullable=True, comment="Location/description inside stadium"
    )
    lat: Mapped[float] = mapped_column(Float, nullable=False, comment="Latitude (stadium fallback)")
    lng: Mapped[float] = mapped_column(Float, nullable=False, comment="Longitude (stadium fallback)")
    address: Mapped[str | None] = mapped_column(String(300), nullable=True, comment="Address")
    phone: Mapped[str | None] = mapped_column(String(30), nullable=True, comment="Contact phone")
    rating: Mapped[float | None] = mapped_column(Numeric, nullable=True, comment="Rating")
    open_time: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Opening time")
    close_time: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Closing time")

    stadium: Mapped[Stadium] = relationship("Stadium", back_populates="places")

    __table_args__ = (Index("idx_places_category", "category"),)

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<Place(stadium='{self.stadium_id}', category='{self.category}', name='{self.name}')>"
