"""Model representing stadium foods and restaurant recommendations."""

from __future__ import annotations

from sqlalchemy import Boolean, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class StadiumFood(Base, TimestampMixin):
    """Represents a food item or restaurant at/near a KBO stadium."""

    __tablename__ = "stadium_foods"
    __table_args__ = (UniqueConstraint("stadium_name", "restaurant_name", "menu_item", name="uq_stadium_food"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stadium_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True, comment="홈 구장 명칭")
    restaurant_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True, comment="식음료 매장명")
    menu_item: Mapped[str] = mapped_column(String(200), nullable=False, comment="대표 메뉴 정보")
    location: Mapped[str | None] = mapped_column(String(200), nullable=True, comment="구장 내/외 상세 위치")
    description: Mapped[str | None] = mapped_column(Text, nullable=True, comment="상세 팁 및 설명")
    is_famous: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", comment="시그니처 메뉴 여부")
    recommended_by: Mapped[str | None] = mapped_column(String(100), nullable=True, comment="추천 주체 (예: 팬, 구단)")

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return (
            f"<StadiumFood(stadium='{self.stadium_name}', restaurant='{self.restaurant_name}', "
            f"menu='{self.menu_item}')>"
        )
