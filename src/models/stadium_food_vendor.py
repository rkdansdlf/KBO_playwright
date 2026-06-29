"""데이터 모델: stadium food vendor."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class StadiumFoodVendor(Base, TimestampMixin):
    """StadiumFoodVendor class."""

    __tablename__ = "stadium_food_vendors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_id: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("stadium_info.stadium_code", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="Stadium code",
    )
    vendor_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Food vendor name")
    location_text: Mapped[str | None] = mapped_column(String(200), nullable=True, comment="Location description")
    floor_level: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Floor level")
    base_side: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        comment="first_base / third_base / center / outfield",
    )
    gate_info: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="Nearby gate")
    order_method: Mapped[str | None] = mapped_column(
        String(20),
        nullable=True,
        default="onsite",
        comment="onsite / app / qr / delivery / unknown",
    )
    source_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("data_sources.id", ondelete="SET NULL"),
        nullable=True,
        comment="DataSource ID",
    )
    confidence: Mapped[str] = mapped_column(String(10), nullable=False, default="medium", comment="high / medium / low")
    last_verified_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
        comment="Last verification timestamp",
    )

    __table_args__ = (
        UniqueConstraint("stadium_id", "vendor_name", name="uq_food_vendor"),
        Index("idx_sfv_confidence", "confidence"),
    )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<StadiumFoodVendor(stadium='{self.stadium_id}', vendor='{self.vendor_name}')>"
