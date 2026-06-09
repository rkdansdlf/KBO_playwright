from __future__ import annotations

from sqlalchemy import Boolean, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class ParkingLot(Base, TimestampMixin):
    __tablename__ = "parking_lots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_id: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("stadium_info.stadium_code", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Stadium code",
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Parking lot name")
    lot_type: Mapped[str] = mapped_column(
        String(20), nullable=False, default="official", comment="official / public / private / temporary",
    )
    address: Mapped[str | None] = mapped_column(String(300), nullable=True, comment="Address")
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Latitude")
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Longitude")
    capacity: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Total parking capacity")
    walking_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Walking minutes to stadium")
    is_event_day_available: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="1", comment="Available on event days",
    )
    reservation_required: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="0", comment="Reservation required",
    )
    operating_hours: Mapped[str | None] = mapped_column(
        String(200), nullable=True, comment="Operating hours description",
    )
    source_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("data_sources.id", ondelete="SET NULL"), nullable=True, comment="DataSource ID",
    )

    __table_args__ = (
        UniqueConstraint("stadium_id", "name", name="uq_parking_lot"),
        Index("idx_pl_type", "lot_type"),
    )

    def __repr__(self) -> str:
        return f"<ParkingLot(stadium='{self.stadium_id}', name='{self.name}')>"
