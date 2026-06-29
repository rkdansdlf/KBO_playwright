from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_congestion import StadiumCongestion
from src.repositories.congestion_repository import CongestionRepository


class TestCongestionRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        StadiumCongestion.__table__.create(engine)

    def test_upsert_creates_record(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        record, created = repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            },
        )
        session.commit()

        assert created is True
        assert record.id is not None
        assert record.congestion_level == "high"

    def test_upsert_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        r1, created1 = repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            },
        )
        session.commit()

        r2, created2 = repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "congestion_index": 90.0,
                "source": "seoul_open_api",
                "people_count": 500,
            },
        )
        session.commit()

        assert created2 is False
        assert r1.id == r2.id
        assert r2.congestion_level == "very_high"
        assert r2.congestion_index == 90.0
        assert r2.people_count == 500

    def test_bulk_upsert(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        created, updated = repo.bulk_upsert(
            [
                {
                    "stadium_code": "JAMSIL",
                    "location_type": "gate",
                    "location_label": "1번게이트",
                    "measured_at": datetime(2024, 10, 15, 18, 30),
                    "game_date": date(2024, 10, 15),
                    "congestion_level": "high",
                    "congestion_index": 75.0,
                    "source": "seoul_open_api",
                },
                {
                    "stadium_code": "JAMSIL",
                    "location_type": "gate",
                    "location_label": "2번게이트",
                    "measured_at": datetime(2024, 10, 15, 18, 30),
                    "game_date": date(2024, 10, 15),
                    "congestion_level": "normal",
                    "congestion_index": 30.0,
                    "source": "seoul_open_api",
                },
            ],
        )
        session.commit()
        assert created == 2
        assert updated == 0

    def test_bulk_upsert_mixed(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            },
        )
        session.commit()

        created, updated = repo.bulk_upsert(
            [
                {
                    "stadium_code": "JAMSIL",
                    "location_type": "gate",
                    "location_label": "1번게이트",
                    "measured_at": datetime(2024, 10, 15, 18, 30),
                    "game_date": date(2024, 10, 15),
                    "congestion_level": "very_high",
                    "congestion_index": 90.0,
                    "source": "seoul_open_api",
                },
                {
                    "stadium_code": "JAMSIL",
                    "location_type": "gate",
                    "location_label": "3번게이트",
                    "measured_at": datetime(2024, 10, 15, 19, 0),
                    "game_date": date(2024, 10, 15),
                    "congestion_level": "low",
                    "congestion_index": 10.0,
                    "source": "seoul_open_api",
                },
            ],
        )
        session.commit()
        assert created == 1
        assert updated == 1

    def test_get_by_game_date(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            },
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 19, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "congestion_index": 90.0,
                "source": "seoul_open_api",
            },
        )
        repo.upsert(
            {
                "stadium_code": "MUNH",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "normal",
                "congestion_index": 40.0,
                "source": "seoul_open_api",
            },
        )
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15))
        assert len(results) == 2

    def test_get_by_game_date_filtered(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            },
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "subway_station",
                "location_label": "잠실역",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "normal",
                "congestion_index": 40.0,
                "source": "seoul_open_api",
            },
        )
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15), location_type="gate")
        assert len(results) == 1

    def test_get_latest(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            },
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 19, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "congestion_index": 90.0,
                "source": "seoul_open_api",
            },
        )
        session.commit()

        latest = repo.get_latest("JAMSIL", "1번게이트")
        assert latest is not None
        assert latest.congestion_level == "very_high"

    def test_get_peak_congestion(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = CongestionRepository(session)

        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "1번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 0),
                "game_date": date(2024, 10, 15),
                "congestion_level": "high",
                "congestion_index": 75.0,
                "source": "seoul_open_api",
            },
        )
        repo.upsert(
            {
                "stadium_code": "JAMSIL",
                "location_type": "gate",
                "location_label": "2번게이트",
                "measured_at": datetime(2024, 10, 15, 18, 30),
                "game_date": date(2024, 10, 15),
                "congestion_level": "very_high",
                "congestion_index": 90.0,
                "source": "seoul_open_api",
            },
        )
        session.commit()

        peak = repo.get_peak_congestion("JAMSIL", date(2024, 10, 15))
        assert peak is not None
        assert peak.congestion_index == 90.0
