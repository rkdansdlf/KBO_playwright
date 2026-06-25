"""데이터 모델: parking fee rule."""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class ParkingFeeRule(Base, TimestampMixin):
    """ParkingFeeRule class."""

    __tablename__ = "parking_fee_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parking_lot_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("parking_lots.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to parking_lots",
    )
    vehicle_type: Mapped[str] = mapped_column(String(20), nullable=False, comment="compact / sedan / van / bus")
    base_fee: Mapped[int] = mapped_column(Integer, nullable=False, comment="Base fee in KRW")
    base_minutes: Mapped[int] = mapped_column(Integer, nullable=False, comment="Base fee duration in minutes")
    additional_fee: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Additional fee per extra unit")
    additional_minutes: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Extra minutes per additional fee unit",
    )
    daily_max_fee: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Daily maximum fee")
    event_flat_fee: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Event day flat fee (instead of regular)",
    )
    discount_json: Mapped[str | None] = mapped_column(Text, nullable=True, comment="JSON string of discount rules")
    free_exit_minutes: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Free exit minutes after payment",
    )

    __table_args__ = (UniqueConstraint("parking_lot_id", "vehicle_type", name="uq_parking_fee"),)

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<ParkingFeeRule(lot_id={self.parking_lot_id}, vehicle='{self.vehicle_type}', fee={self.base_fee})>"
