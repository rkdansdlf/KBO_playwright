"""데이터 모델: stadium info."""

from __future__ import annotations

from sqlalchemy import JSON, Boolean, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class StadiumInfo(Base, TimestampMixin):
    __tablename__ = "stadium_info"

    stadium_code: Mapped[str] = mapped_column(
        String(10),
        primary_key=True,
        comment="Canonical stadium code (e.g. JAMSIL, MUNHAK)",
    )
    name_kr: Mapped[str] = mapped_column(String(100), nullable=False, comment="Stadium name in Korean")
    name_en: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Stadium name in English")
    home_team_id: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="Home team code")
    capacity: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Seating capacity")
    opened_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Year opened")
    location: Mapped[str | None] = mapped_column(String(200), nullable=True, comment="City/district location")
    address: Mapped[str | None] = mapped_column(String(300), nullable=True, comment="Full address")
    parking_info: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Parking details")
    public_transport: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Public transit info {subway, bus}",
    )
    facilities: Mapped[dict | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Facilities list {restaurant, shop, etc}",
    )
    seat_map_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="Seat map URL")
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Latitude")
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Longitude")
    is_dome: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", comment="Is domed stadium")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1", comment="Currently in use")

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<StadiumInfo(code='{self.stadium_code}', name='{self.name_kr}')>"


class StadiumRegulation(Base, TimestampMixin):
    __tablename__ = "stadium_regulations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_code: Mapped[str] = mapped_column(String(10), nullable=False, index=True, comment="Canonical stadium code")
    regulation_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Type: GROUND_RULE/ADVERTISING/DUGOUT/ETC",
    )
    title: Mapped[str] = mapped_column(String(200), nullable=False, comment="Regulation title")
    description: Mapped[str] = mapped_column(Text, nullable=False, comment="Detailed regulation description")
    source: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Source of regulation")

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<StadiumRegulation(stadium='{self.stadium_code}', type='{self.regulation_type}')>"
