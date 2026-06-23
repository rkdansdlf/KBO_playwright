"""
Repository for ParkingLot and ParkingFeeRule operations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from ..models.parking_fee_rule import ParkingFeeRule
from ..models.parking_lot import ParkingLot

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class ParkingLotRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> ParkingLot:
        stadium_id = data["stadium_id"]
        name = data["name"]
        stmt = select(ParkingLot).where(
            ParkingLot.stadium_id == stadium_id,
            ParkingLot.name == name,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("stadium_id", "name") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = ParkingLot(**data)
        self.session.add(new_record)
        self.session.flush()
        return new_record

    def get_by_stadium(self, stadium_id: str) -> list[ParkingLot]:
        stmt = (
            select(ParkingLot).where(ParkingLot.stadium_id == stadium_id).order_by(ParkingLot.lot_type, ParkingLot.name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def bulk_save(self, records: list[dict]) -> int:
        count = 0
        for data in records:
            self.save(data)
            count += 1
        return count


class ParkingFeeRuleRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save(self, data: dict) -> ParkingFeeRule:
        lot_id = data["parking_lot_id"]
        vehicle = data["vehicle_type"]
        stmt = select(ParkingFeeRule).where(
            ParkingFeeRule.parking_lot_id == lot_id,
            ParkingFeeRule.vehicle_type == vehicle,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            for key, value in data.items():
                if key not in ("parking_lot_id", "vehicle_type") and value is not None:
                    setattr(existing, key, value)
            return existing
        new_record = ParkingFeeRule(**data)
        self.session.add(new_record)
        return new_record

    def get_by_lot(self, parking_lot_id: int) -> list[ParkingFeeRule]:
        stmt = (
            select(ParkingFeeRule)
            .where(ParkingFeeRule.parking_lot_id == parking_lot_id)
            .order_by(ParkingFeeRule.vehicle_type)
        )
        return list(self.session.execute(stmt).scalars().all())
