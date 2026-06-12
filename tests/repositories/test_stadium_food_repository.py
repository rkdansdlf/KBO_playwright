from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.repositories.stadium_food_repository import StadiumFoodMenuItemRepository, StadiumFoodVendorRepository


class TestStadiumFoodVendorRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        StadiumFoodVendor.__table__.create(engine)

    def test_save_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodVendorRepository(session)

        vendor = repo.save(
            {
                "stadium_id": "JAMSIL",
                "vendor_name": "잠실푸드",
                "location_text": "1층",
                "confidence": "high",
            }
        )
        session.commit()
        assert vendor.id is not None
        assert vendor.vendor_name == "잠실푸드"

    def test_save_upsert_same_key(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodVendorRepository(session)

        r1 = repo.save(
            {
                "stadium_id": "JAMSIL",
                "vendor_name": "잠실푸드",
                "location_text": "1층",
                "confidence": "medium",
            }
        )
        session.commit()

        r2 = repo.save(
            {
                "stadium_id": "JAMSIL",
                "vendor_name": "잠실푸드",
                "location_text": "2층",
                "confidence": "high",
            }
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.location_text == "2층"

    def test_get_by_stadium(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodVendorRepository(session)

        repo.save({"stadium_id": "JAMSIL", "vendor_name": "A", "confidence": "high"})
        repo.save({"stadium_id": "JAMSIL", "vendor_name": "B", "confidence": "medium"})
        repo.save({"stadium_id": "MUNH", "vendor_name": "C", "confidence": "high"})
        session.commit()

        results = repo.get_by_stadium("JAMSIL")
        assert len(results) == 2

    def test_bulk_save(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodVendorRepository(session)

        count = repo.bulk_save(
            [
                {"stadium_id": "JAMSIL", "vendor_name": "A", "confidence": "high"},
                {"stadium_id": "JAMSIL", "vendor_name": "B", "confidence": "medium"},
            ]
        )
        session.commit()
        assert count == 2


class TestStadiumFoodMenuItemRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        StadiumFoodVendor.__table__.create(engine)
        StadiumFoodMenuItem.__table__.create(engine)

    def _create_vendor(self, session):
        vendor = StadiumFoodVendor(stadium_id="JAMSIL", vendor_name="잠실푸드", confidence="high")
        session.add(vendor)
        session.flush()
        return vendor.id

    def test_save_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodMenuItemRepository(session)

        vendor_id = self._create_vendor(session)
        item = repo.save(
            {
                "vendor_id": vendor_id,
                "menu_name": "떡볶이",
                "price": 5000,
                "category": "snack",
            }
        )
        session.commit()
        assert item.id is not None
        assert item.menu_name == "떡볶이"

    def test_save_upsert_same_key(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodMenuItemRepository(session)

        vendor_id = self._create_vendor(session)
        r1 = repo.save(
            {
                "vendor_id": vendor_id,
                "menu_name": "떡볶이",
                "price": 5000,
                "category": "snack",
            }
        )
        session.commit()

        r2 = repo.save(
            {
                "vendor_id": vendor_id,
                "menu_name": "떡볶이",
                "price": 5500,
                "category": "snack",
                "is_signature": True,
            }
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.price == 5500
        assert r2.is_signature is True

    def test_get_by_vendor(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodMenuItemRepository(session)

        vendor_id = self._create_vendor(session)
        repo.save({"vendor_id": vendor_id, "menu_name": "떡볶이", "price": 5000, "category": "snack"})
        repo.save({"vendor_id": vendor_id, "menu_name": "김밥", "price": 3000, "category": "snack"})
        session.commit()

        results = repo.get_by_vendor(vendor_id)
        assert len(results) == 2

    def test_get_by_stadium(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodMenuItemRepository(session)

        vendor_id = self._create_vendor(session)
        repo.save({"vendor_id": vendor_id, "menu_name": "떡볶이", "price": 5000, "category": "snack"})
        session.commit()

        results = repo.get_by_stadium("JAMSIL")
        assert len(results) == 1
        assert results[0]["menu_name"] == "떡볶이"
        assert results[0]["vendor_name"] == "잠실푸드"

    def test_bulk_save(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = StadiumFoodMenuItemRepository(session)

        vendor_id = self._create_vendor(session)
        count = repo.bulk_save(
            [
                {"vendor_id": vendor_id, "menu_name": "떡볶이", "price": 5000, "category": "snack"},
                {"vendor_id": vendor_id, "menu_name": "김밥", "price": 3000, "category": "snack"},
            ]
        )
        session.commit()
        assert count == 2
