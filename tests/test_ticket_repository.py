from datetime import time

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.ticket_open_rule import TicketOpenRule
from src.models.ticket_price import TicketPrice
from src.repositories.ticket_open_rule_repository import TicketOpenRuleRepository
from src.repositories.ticket_price_repository import TicketPriceRepository


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    TicketPrice.__table__.create(engine)
    TicketOpenRule.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


class TestTicketPriceRepository:
    def test_save_and_query(self, session):
        repo = TicketPriceRepository(session)
        data = {
            "team_id": "LG",
            "stadium_id": "JAMSIL",
            "season": 2025,
            "seat_grade": "블루석",
            "day_type": "weekday",
            "price": 12000,
        }
        tp = repo.save(data)
        assert tp.seat_grade == "블루석"
        assert tp.price == 12000

    def test_save_dedup(self, session):
        repo = TicketPriceRepository(session)
        data = {
            "team_id": "LG",
            "stadium_id": "JAMSIL",
            "season": 2025,
            "seat_grade": "블루석",
            "day_type": "weekday",
            "price": 12000,
        }
        e1 = repo.save(data)
        e2 = repo.save({**data, "price": 15000})
        assert e1.id == e2.id
        assert e2.price == 15000

    def test_get_by_team_season(self, session):
        repo = TicketPriceRepository(session)
        repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2025,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "price": 12000,
            }
        )
        repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2025,
                "seat_grade": "레드석",
                "day_type": "weekday",
                "price": 18000,
            }
        )
        repo.save(
            {
                "team_id": "SS",
                "stadium_id": "DAEGU",
                "season": 2025,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "price": 10000,
            }
        )
        session.commit()

        lg_prices = repo.get_by_team_season("LG", 2025)
        assert len(lg_prices) == 2

    def test_get_by_stadium_season(self, session):
        repo = TicketPriceRepository(session)
        repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2025,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "price": 12000,
            }
        )
        repo.save(
            {
                "team_id": "OB",
                "stadium_id": "JAMSIL",
                "season": 2025,
                "seat_grade": "레드석",
                "day_type": "weekday",
                "price": 15000,
            }
        )
        session.commit()

        jamsil = repo.get_by_stadium_season("JAMSIL", 2025)
        assert len(jamsil) == 2

    def test_bulk_save(self, session):
        repo = TicketPriceRepository(session)
        records = [
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2025,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "price": 12000,
            },
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2025,
                "seat_grade": "레드석",
                "day_type": "weekday",
                "price": 18000,
            },
        ]
        count = repo.bulk_save(records)
        assert count == 2

    def test_day_type_weekend(self, session):
        repo = TicketPriceRepository(session)
        repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2025,
                "seat_grade": "블루석",
                "day_type": "weekend",
                "price": 15000,
            }
        )
        weekend = repo.get_by_team_season("LG", 2025)
        assert len(weekend) == 1
        assert weekend[0].day_type == "weekend"


class TestTicketOpenRuleRepository:
    def test_save_and_query(self, session):
        repo = TicketOpenRuleRepository(session)
        data = {"team_id": "LG", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)}
        rule = repo.save(data)
        assert rule.team_id == "LG"
        assert rule.open_offset_days == 7

    def test_save_dedup(self, session):
        repo = TicketOpenRuleRepository(session)
        data = {"team_id": "LG", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)}
        e1 = repo.save(data)
        e2 = repo.save({**data, "note": "업데이트"})
        assert e1.id == e2.id
        assert e2.note == "업데이트"

    def test_get_by_team(self, session):
        repo = TicketOpenRuleRepository(session)
        repo.save({"team_id": "LG", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)})
        repo.save({"team_id": "LG", "platform": "Interpark", "open_offset_days": 3, "open_time": time(14, 0)})
        repo.save({"team_id": "SS", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)})
        session.commit()

        lg_rules = repo.get_by_team("LG")
        assert len(lg_rules) == 2

    def test_get_all_active(self, session):
        repo = TicketOpenRuleRepository(session)
        repo.save({"team_id": "LG", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)})
        repo.save({"team_id": "SS", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)})
        session.commit()

        all_rules = repo.get_all_active()
        assert len(all_rules) == 2

    def test_bulk_save(self, session):
        repo = TicketOpenRuleRepository(session)
        records = [
            {"team_id": "LG", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)},
            {"team_id": "SS", "platform": "Ticketlink", "open_offset_days": 7, "open_time": time(11, 0)},
        ]
        count = repo.bulk_save(records)
        assert count == 2
