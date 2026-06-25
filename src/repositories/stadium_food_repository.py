"""Repository for StadiumFoodVendor and StadiumFoodMenuItem operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class StadiumFoodVendorRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> StadiumFoodVendor:
        stadium_id = data["stadium_id"]
        vendor_name = data["vendor_name"]
        stmt = select(StadiumFoodVendor).where(
            StadiumFoodVendor.stadium_id == stadium_id,
            StadiumFoodVendor.vendor_name == vendor_name,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("stadium_id", "vendor_name") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = StadiumFoodVendor(**data)
        self.session.add(new_record)
        self.session.flush()
        return new_record

    def get_by_stadium(self, stadium_id: str) -> list[StadiumFoodVendor]:
        stmt = (
            select(StadiumFoodVendor)
            .where(StadiumFoodVendor.stadium_id == stadium_id)
            .order_by(StadiumFoodVendor.vendor_name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def bulk_save(self, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.save(data)
            count += 1
        return count


class StadiumFoodMenuItemRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> StadiumFoodMenuItem:
        vendor_id = data["vendor_id"]
        menu_name = data["menu_name"]
        stmt = select(StadiumFoodMenuItem).where(
            StadiumFoodMenuItem.vendor_id == vendor_id,
            StadiumFoodMenuItem.menu_name == menu_name,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("vendor_id", "menu_name") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = StadiumFoodMenuItem(**data)
        self.session.add(new_record)
        return new_record

    def get_by_vendor(self, vendor_id: int) -> list[StadiumFoodMenuItem]:
        stmt = (
            select(StadiumFoodMenuItem)
            .where(StadiumFoodMenuItem.vendor_id == vendor_id)
            .order_by(StadiumFoodMenuItem.category, StadiumFoodMenuItem.menu_name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_by_stadium(self, stadium_id: str) -> list[dict]:
        stmt = (
            select(
                StadiumFoodVendor.vendor_name,
                StadiumFoodMenuItem.menu_name,
                StadiumFoodMenuItem.price,
                StadiumFoodMenuItem.category,
                StadiumFoodMenuItem.is_signature,
            )
            .join(StadiumFoodMenuItem, StadiumFoodVendor.id == StadiumFoodMenuItem.vendor_id)
            .where(StadiumFoodVendor.stadium_id == stadium_id)
            .order_by(StadiumFoodVendor.vendor_name, StadiumFoodMenuItem.menu_name)
        )
        rows = self.session.execute(stmt).all()
        return [dict(row._mapping) for row in rows]

    def bulk_save(self, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.save(data)
            count += 1
        return count
