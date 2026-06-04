import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.parking_fee_rule import ParkingFeeRule
from src.models.parking_lot import ParkingLot
from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.models.stadium_info import StadiumInfo
from src.models.stadium_seat_section import StadiumSeatSection
from src.repositories.parking_lot_repository import ParkingFeeRuleRepository, ParkingLotRepository
from src.repositories.stadium_food_repository import StadiumFoodMenuItemRepository, StadiumFoodVendorRepository
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    StadiumInfo.__table__.create(engine)
    StadiumSeatSection.__table__.create(engine)
    ParkingLot.__table__.create(engine)
    ParkingFeeRule.__table__.create(engine)
    StadiumFoodVendor.__table__.create(engine)
    StadiumFoodMenuItem.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


@pytest.fixture
def stadium(session):
    st = StadiumInfo(stadium_code="JAMSIL", name_kr="잠실종합운동장", home_team_id="LG")
    session.add(st)
    session.commit()
    return st


class TestStadiumSeatSectionRepository:
    def test_save_and_get_by_stadium(self, session, stadium):
        repo = StadiumSeatSectionRepository(session)
        data = {"stadium_id": "JAMSIL", "section_name": "블루석", "seat_grade": "블루석", "base_side": "first_base"}
        ss = repo.save(data)
        assert ss.section_name == "블루석"

        sections = repo.get_by_stadium("JAMSIL")
        assert len(sections) == 1
        assert sections[0].section_name == "블루석"

    def test_save_dedup_by_name(self, session, stadium):
        repo = StadiumSeatSectionRepository(session)
        data = {"stadium_id": "JAMSIL", "section_name": "레드석"}
        e1 = repo.save(data)
        e2 = repo.save({**data, "base_side": "third_base"})
        assert e1.id == e2.id
        assert e2.base_side == "third_base"

    def test_get_cheering_sections(self, session, stadium):
        repo = StadiumSeatSectionRepository(session)
        repo.save({"stadium_id": "JAMSIL", "section_name": "응원석", "is_home_cheering": True})
        repo.save({"stadium_id": "JAMSIL", "section_name": "일반석"})
        session.commit()

        cheering = repo.get_cheering_sections("JAMSIL")
        assert len(cheering) == 1

    def test_bulk_save(self, session, stadium):
        repo = StadiumSeatSectionRepository(session)
        records = [
            {"stadium_id": "JAMSIL", "section_name": "A석"},
            {"stadium_id": "JAMSIL", "section_name": "B석"},
        ]
        count = repo.bulk_save(records)
        assert count == 2


class TestParkingLotRepository:
    def test_save_and_get_by_stadium(self, session, stadium):
        repo = ParkingLotRepository(session)
        data = {"stadium_id": "JAMSIL", "name": "잠실주차장", "lot_type": "official", "capacity": 500}
        lot = repo.save(data)
        assert lot.name == "잠실주차장"
        assert lot.id is not None

        lots = repo.get_by_stadium("JAMSIL")
        assert len(lots) == 1

    def test_save_dedup(self, session, stadium):
        repo = ParkingLotRepository(session)
        data = {"stadium_id": "JAMSIL", "name": "공영주차장"}
        e1 = repo.save(data)
        e2 = repo.save({**data, "capacity": 200})
        assert e1.id == e2.id

    def test_bulk_save(self, session, stadium):
        repo = ParkingLotRepository(session)
        records = [
            {"stadium_id": "JAMSIL", "name": "P1"},
            {"stadium_id": "JAMSIL", "name": "P2"},
        ]
        count = repo.bulk_save(records)
        assert count == 2


class TestParkingFeeRuleRepository:
    def test_save_and_get_by_lot(self, session, stadium):
        lot_repo = ParkingLotRepository(session)
        lot = lot_repo.save({"stadium_id": "JAMSIL", "name": "테스트주차장"})

        fee_repo = ParkingFeeRuleRepository(session)
        data = {"parking_lot_id": lot.id, "vehicle_type": "sedan", "base_fee": 2000, "base_minutes": 30}
        rule = fee_repo.save(data)
        assert rule.base_fee == 2000

        rules = fee_repo.get_by_lot(lot.id)
        assert len(rules) == 1
        assert rules[0].vehicle_type == "sedan"

    def test_save_dedup(self, session, stadium):
        lot_repo = ParkingLotRepository(session)
        lot = lot_repo.save({"stadium_id": "JAMSIL", "name": "주차장"})

        fee_repo = ParkingFeeRuleRepository(session)
        data = {"parking_lot_id": lot.id, "vehicle_type": "sedan", "base_fee": 2000, "base_minutes": 30}
        e1 = fee_repo.save(data)
        e2 = fee_repo.save({**data, "daily_max_fee": 10000})
        assert e1.id == e2.id
        assert e2.daily_max_fee == 10000


class TestStadiumFoodVendorRepository:
    def test_save_and_get_by_stadium(self, session, stadium):
        repo = StadiumFoodVendorRepository(session)
        data = {"stadium_id": "JAMSIL", "vendor_name": "맛있는집", "confidence": "high"}
        vendor = repo.save(data)
        assert vendor.vendor_name == "맛있는집"

        vendors = repo.get_by_stadium("JAMSIL")
        assert len(vendors) == 1

    def test_save_dedup(self, session, stadium):
        repo = StadiumFoodVendorRepository(session)
        data = {"stadium_id": "JAMSIL", "vendor_name": "테스트식당"}
        e1 = repo.save(data)
        e2 = repo.save({**data, "location_text": "1층"})
        assert e1.id == e2.id
        assert e2.location_text == "1층"

    def test_bulk_save(self, session, stadium):
        repo = StadiumFoodVendorRepository(session)
        records = [
            {"stadium_id": "JAMSIL", "vendor_name": "V1"},
            {"stadium_id": "JAMSIL", "vendor_name": "V2"},
        ]
        count = repo.bulk_save(records)
        assert count == 2


class TestStadiumFoodMenuItemRepository:
    def test_save_and_get_by_vendor(self, session, stadium):
        vendor_repo = StadiumFoodVendorRepository(session)
        vendor = vendor_repo.save({"stadium_id": "JAMSIL", "vendor_name": "테스트식당"})

        menu_repo = StadiumFoodMenuItemRepository(session)
        data = {"vendor_id": vendor.id, "menu_name": "치킨", "price": 15000, "category": "chicken"}
        item = menu_repo.save(data)
        assert item.menu_name == "치킨"

        items = menu_repo.get_by_vendor(vendor.id)
        assert len(items) == 1

    def test_save_dedup(self, session, stadium):
        vendor_repo = StadiumFoodVendorRepository(session)
        vendor = vendor_repo.save({"stadium_id": "JAMSIL", "vendor_name": "식당"})

        menu_repo = StadiumFoodMenuItemRepository(session)
        data = {"vendor_id": vendor.id, "menu_name": "김밥", "price": 3000}
        e1 = menu_repo.save(data)
        e2 = menu_repo.save({**data, "is_signature": True})
        assert e1.id == e2.id
        assert e2.is_signature is True

    def test_get_by_stadium(self, session, stadium):
        vendor_repo = StadiumFoodVendorRepository(session)
        v1 = vendor_repo.save({"stadium_id": "JAMSIL", "vendor_name": "식당1"})
        v2 = vendor_repo.save({"stadium_id": "JAMSIL", "vendor_name": "식당2"})

        menu_repo = StadiumFoodMenuItemRepository(session)
        menu_repo.save({"vendor_id": v1.id, "menu_name": "메뉴A", "category": "snack"})
        menu_repo.save({"vendor_id": v2.id, "menu_name": "메뉴B", "category": "drink"})

        items = menu_repo.get_by_stadium("JAMSIL")
        assert len(items) == 2

    def test_bulk_save(self, session, stadium):
        vendor_repo = StadiumFoodVendorRepository(session)
        vendor = vendor_repo.save({"stadium_id": "JAMSIL", "vendor_name": "식당"})

        menu_repo = StadiumFoodMenuItemRepository(session)
        records = [
            {"vendor_id": vendor.id, "menu_name": "M1"},
            {"vendor_id": vendor.id, "menu_name": "M2"},
        ]
        count = menu_repo.bulk_save(records)
        assert count == 2
