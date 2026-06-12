from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.parking_fee_rule import ParkingFeeRule
from src.models.parking_lot import ParkingLot
from src.repositories.parking_lot_repository import ParkingFeeRuleRepository, ParkingLotRepository


class TestParkingLotRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        ParkingLot.__table__.create(engine)

    def test_save_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingLotRepository(session)

        lot = repo.save(
            {
                "stadium_id": "JAMSIL",
                "name": "잠실주차장",
                "lot_type": "official",
                "capacity": 500,
            }
        )
        session.commit()
        assert lot.id is not None
        assert lot.name == "잠실주차장"

    def test_save_upsert_same_key(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingLotRepository(session)

        r1 = repo.save(
            {
                "stadium_id": "JAMSIL",
                "name": "잠실주차장",
                "lot_type": "official",
                "capacity": 500,
            }
        )
        session.commit()

        r2 = repo.save(
            {
                "stadium_id": "JAMSIL",
                "name": "잠실주차장",
                "lot_type": "official",
                "capacity": 600,
                "operating_hours": "09:00-22:00",
            }
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.capacity == 600

    def test_get_by_stadium(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingLotRepository(session)

        repo.save({"stadium_id": "JAMSIL", "name": "A", "lot_type": "official"})
        repo.save({"stadium_id": "JAMSIL", "name": "B", "lot_type": "public"})
        repo.save({"stadium_id": "MUNH", "name": "C", "lot_type": "official"})
        session.commit()

        results = repo.get_by_stadium("JAMSIL")
        assert len(results) == 2

    def test_bulk_save(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingLotRepository(session)

        count = repo.bulk_save(
            [
                {"stadium_id": "JAMSIL", "name": "A", "lot_type": "official"},
                {"stadium_id": "JAMSIL", "name": "B", "lot_type": "public"},
            ]
        )
        session.commit()
        assert count == 2


class TestParkingFeeRuleRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        ParkingLot.__table__.create(engine)
        ParkingFeeRule.__table__.create(engine)

    def _create_lot(self, session):
        lot = ParkingLot(stadium_id="JAMSIL", name="잠실주차장", lot_type="official")
        session.add(lot)
        session.flush()
        return lot.id

    def test_save_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingFeeRuleRepository(session)

        lot_id = self._create_lot(session)
        rule = repo.save(
            {
                "parking_lot_id": lot_id,
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
            }
        )
        session.commit()
        assert rule.id is not None
        assert rule.base_fee == 2000

    def test_save_upsert_same_key(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingFeeRuleRepository(session)

        lot_id = self._create_lot(session)
        r1 = repo.save(
            {
                "parking_lot_id": lot_id,
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
            }
        )
        session.commit()

        r2 = repo.save(
            {
                "parking_lot_id": lot_id,
                "vehicle_type": "sedan",
                "base_fee": 2500,
                "base_minutes": 30,
                "daily_max_fee": 10000,
            }
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.base_fee == 2500
        assert r2.daily_max_fee == 10000

    def test_get_by_lot(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingFeeRuleRepository(session)

        lot_id = self._create_lot(session)
        repo.save({"parking_lot_id": lot_id, "vehicle_type": "sedan", "base_fee": 2000, "base_minutes": 30})
        repo.save({"parking_lot_id": lot_id, "vehicle_type": "compact", "base_fee": 1000, "base_minutes": 30})
        session.commit()

        results = repo.get_by_lot(lot_id)
        assert len(results) == 2

    def test_get_by_lot_empty(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = ParkingFeeRuleRepository(session)

        assert repo.get_by_lot(999) == []
