"""데이터 모델: stadium food menu item."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Boolean, Date, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class StadiumFoodMenuItem(Base, TimestampMixin):
    __tablename__ = "stadium_food_menu_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    vendor_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("stadium_food_vendors.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK to stadium_food_vendors",
    )
    menu_name: Mapped[str] = mapped_column(String(200), nullable=False, comment="Menu item name")
    price: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Price in KRW")
    category: Mapped[str | None] = mapped_column(
        String(30),
        nullable=True,
        comment="chicken / snack / meal / dessert / drink / beer / etc",
    )
    is_signature: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default="0",
        comment="Signature/recommended item",
    )
    tags_json: Mapped[str | None] = mapped_column(Text, nullable=True, comment="JSON array of tags")
    source_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("data_sources.id", ondelete="SET NULL"),
        nullable=True,
        comment="DataSource ID",
    )
    effective_from: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Menu available from")
    effective_to: Mapped[date | None] = mapped_column(Date, nullable=True, comment="Menu available until")

    __table_args__ = (UniqueConstraint("vendor_id", "menu_name", name="uq_food_menu_item"),)

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<StadiumFoodMenuItem(vendor_id={self.vendor_id}, menu='{self.menu_name}')>"
