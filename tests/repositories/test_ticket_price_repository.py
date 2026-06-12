from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.ticket_price import TicketPrice
from src.repositories.ticket_price_repository import TicketPriceRepository


class TestTicketPriceRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        TicketPrice.__table__.create(engine)

    def test_save_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketPriceRepository(session)

        tp = repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2024,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "audience_type": None,
                "price": 12000,
            }
        )
        session.commit()

        assert tp.id is not None
        assert tp.price == 12000

    def test_save_upsert_same_key_updates(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketPriceRepository(session)

        r1 = repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2024,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "audience_type": None,
                "price": 12000,
            }
        )
        session.commit()

        r2 = repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2024,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "audience_type": None,
                "price": 13000,
                "currency": "KRW",
            }
        )
        session.commit()

        assert r1.id == r2.id
        assert r2.price == 13000

    def test_get_by_team_season(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketPriceRepository(session)

        repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2024,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "audience_type": None,
                "price": 12000,
            }
        )
        repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2024,
                "seat_grade": "레드석",
                "day_type": "weekend",
                "audience_type": None,
                "price": 18000,
            }
        )
        repo.save(
            {
                "team_id": "SSG",
                "stadium_id": "MUNH",
                "season": 2024,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "audience_type": None,
                "price": 11000,
            }
        )
        session.commit()

        results = repo.get_by_team_season("LG", 2024)
        assert len(results) == 2

    def test_get_by_stadium_season(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketPriceRepository(session)

        repo.save(
            {
                "team_id": "LG",
                "stadium_id": "JAMSIL",
                "season": 2024,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "audience_type": None,
                "price": 12000,
            }
        )
        repo.save(
            {
                "team_id": "SSG",
                "stadium_id": "JAMSIL",
                "season": 2024,
                "seat_grade": "블루석",
                "day_type": "weekday",
                "audience_type": None,
                "price": 11000,
            }
        )
        session.commit()

        results = repo.get_by_stadium_season("JAMSIL", 2024)
        assert len(results) == 2

    def test_bulk_save(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = TicketPriceRepository(session)

        count = repo.bulk_save(
            [
                {
                    "team_id": "LG",
                    "stadium_id": "JAMSIL",
                    "season": 2024,
                    "seat_grade": "블루석",
                    "day_type": "weekday",
                    "audience_type": None,
                    "price": 12000,
                },
                {
                    "team_id": "LG",
                    "stadium_id": "JAMSIL",
                    "season": 2024,
                    "seat_grade": "레드석",
                    "day_type": "weekend",
                    "audience_type": None,
                    "price": 18000,
                },
            ]
        )
        session.commit()
        assert count == 2
