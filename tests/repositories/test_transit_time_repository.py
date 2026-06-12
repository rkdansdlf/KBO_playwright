from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_transit_time import StadiumTransitTime
from src.repositories.transit_time_repository import TransitTimeRepository


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    StadiumTransitTime.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def _make_data(overrides=None) -> dict:
    base = {
        "stadium_code": "JAMSIL",
        "origin_label": "잠실역_2호선",
        "transport_mode": "subway",
        "measured_at": datetime(2024, 10, 15, 18, 30),
        "game_date": date(2024, 10, 15),
        "duration_minutes": 25,
        "distance_meters": 1200,
        "source_api": "kakao",
        "congestion_factor": 1.2,
        "raw_response": {"status": "ok"},
    }
    if overrides:
        base.update(overrides)
    return base


class TestTransitTimeRepository:
    def test_upsert_create(self, session):
        repo = TransitTimeRepository(session)
        data = _make_data()
        record, created = repo.upsert(data)
        assert created
        assert record.stadium_code == "JAMSIL"
        assert record.duration_minutes == 25
        assert record.source_api == "kakao"
        assert record.origin_label == "잠실역_2호선"

    def test_upsert_update(self, session):
        repo = TransitTimeRepository(session)
        data = _make_data()
        repo.upsert(data)
        session.commit()

        update_data = _make_data({"duration_minutes": 35, "congestion_factor": 1.5})
        record, created = repo.upsert(update_data)
        assert not created
        assert record.duration_minutes == 35
        assert record.congestion_factor == 1.5

    def test_upsert_update_only_mutable_fields(self, session):
        repo = TransitTimeRepository(session)
        data = _make_data()
        repo.upsert(data)
        session.commit()

        update_data = _make_data({"duration_minutes": 40, "source_api": None})
        record, created = repo.upsert(update_data)
        assert not created
        assert record.duration_minutes == 40
        assert record.source_api == "kakao"

    def test_bulk_upsert(self, session):
        repo = TransitTimeRepository(session)
        records = [
            _make_data({"origin_label": "잠실역_2호선", "duration_minutes": 20}),
            _make_data({"origin_label": "잠실역_8호선", "duration_minutes": 30}),
            _make_data({"origin_label": "종합운동장역", "duration_minutes": 15}),
        ]
        created, updated = repo.bulk_upsert(records)
        assert created == 3
        assert updated == 0

        created, updated = repo.bulk_upsert(records)
        assert created == 0
        assert updated == 3

    def test_get_by_game_date(self, session):
        repo = TransitTimeRepository(session)
        repo.upsert(_make_data({"origin_label": "A", "game_date": date(2024, 10, 15)}))
        repo.upsert(_make_data({"origin_label": "B", "game_date": date(2024, 10, 15)}))
        repo.upsert(_make_data({"origin_label": "C", "game_date": date(2024, 10, 16)}))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15))
        assert len(results) == 2

    def test_get_by_game_date_with_filters(self, session):
        repo = TransitTimeRepository(session)
        repo.upsert(_make_data({"origin_label": "A", "game_date": date(2024, 10, 15)}))
        repo.upsert(_make_data({"origin_label": "A", "game_date": date(2024, 10, 15), "transport_mode": "bus"}))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15), origin_label="A", transport_mode="subway")
        assert len(results) == 1
        assert results[0].transport_mode == "subway"

    def test_get_latest(self, session):
        repo = TransitTimeRepository(session)
        repo.upsert(_make_data({"measured_at": datetime(2024, 10, 15, 10, 0), "duration_minutes": 20}))
        repo.upsert(_make_data({"measured_at": datetime(2024, 10, 15, 18, 0), "duration_minutes": 30}))
        session.commit()

        latest = repo.get_latest("JAMSIL", "잠실역_2호선", "subway")
        assert latest is not None
        assert latest.duration_minutes == 30

    def test_get_latest_none(self, session):
        repo = TransitTimeRepository(session)
        latest = repo.get_latest("NONE", "nowhere", "walk")
        assert latest is None

    def test_get_avg_duration(self, session):
        repo = TransitTimeRepository(session)
        repo.upsert(
            _make_data(
                {
                    "game_date": date(2024, 10, 15),
                    "duration_minutes": 20,
                    "measured_at": datetime(2024, 10, 15, 10, 0),
                }
            )
        )
        repo.upsert(
            _make_data(
                {
                    "game_date": date(2024, 10, 15),
                    "duration_minutes": 30,
                    "measured_at": datetime(2024, 10, 15, 18, 0),
                }
            )
        )
        session.commit()

        avg = repo.get_avg_duration("JAMSIL", "잠실역_2호선", date(2024, 10, 15))
        assert avg == 25.0

    def test_get_avg_duration_with_mode(self, session):
        repo = TransitTimeRepository(session)
        repo.upsert(_make_data({"game_date": date(2024, 10, 15), "duration_minutes": 20, "transport_mode": "subway"}))
        repo.upsert(_make_data({"game_date": date(2024, 10, 15), "duration_minutes": 40, "transport_mode": "bus"}))
        session.commit()

        avg = repo.get_avg_duration("JAMSIL", "잠실역_2호선", date(2024, 10, 15), transport_mode="subway")
        assert avg == 20.0

    def test_get_avg_duration_no_data(self, session):
        repo = TransitTimeRepository(session)
        avg = repo.get_avg_duration("NONE", "nothing", date(2024, 10, 15))
        assert avg is None
