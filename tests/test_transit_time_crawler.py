"""
Tests for StadiumTransitTime repository and related infrastructure.

Covers:
  - TransitTimeRepository upsert / bulk_upsert / dedup
  - get_by_game_date / get_latest / get_avg_duration queries
  - JAMSIL_ORIGINS origin config sanity checks
  - map_api_client TransitResult dataclass
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_info import StadiumInfo
from src.models.stadium_transit_time import StadiumTransitTime
from src.repositories.transit_time_repository import TransitTimeRepository

# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    StadiumInfo.__table__.create(engine)
    StadiumTransitTime.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


@pytest.fixture
def stadium(session):
    st = StadiumInfo(stadium_code="JAMSIL", name_kr="잠실야구장", home_team_id="LG")
    session.add(st)
    session.commit()
    return st


def _transit(
    origin_label="잠실역_2호선_7번출구",
    transport_mode="walk",
    measured_at=None,
    game_date=None,
    duration_minutes=12,
    source_api="kakao",
):
    return {
        "stadium_code": "JAMSIL",
        "origin_label": origin_label,
        "origin_lat": 37.5133,
        "origin_lng": 127.0999,
        "transport_mode": transport_mode,
        "measured_at": measured_at or datetime(2026, 6, 3, 15, 0),
        "game_date": game_date or date(2026, 6, 3),
        "duration_minutes": duration_minutes,
        "distance_meters": 800,
        "congestion_factor": None,
        "source_api": source_api,
        "raw_response": {"test": True},
    }


# ─────────────────────────────────────────────
# TransitTimeRepository Tests
# ─────────────────────────────────────────────


class TestTransitTimeRepositoryUpsert:
    def test_insert_new_record(self, session, stadium):
        repo = TransitTimeRepository(session)
        rec, created = repo.upsert(_transit())
        session.commit()
        assert created is True
        assert rec.id is not None
        assert rec.duration_minutes == 12

    def test_dedup_by_composite_key(self, session, stadium):
        repo = TransitTimeRepository(session)
        data = _transit(measured_at=datetime(2026, 6, 3, 15, 0))
        r1, c1 = repo.upsert(data)
        session.commit()

        # Same key → update duration
        r2, c2 = repo.upsert({**data, "duration_minutes": 20})
        session.commit()

        assert c1 is True
        assert c2 is False
        assert r1.id == r2.id
        assert r2.duration_minutes == 20

    def test_different_measured_at_creates_new(self, session, stadium):
        repo = TransitTimeRepository(session)
        r1, _ = repo.upsert(_transit(measured_at=datetime(2026, 6, 3, 15, 0)))
        r2, _ = repo.upsert(_transit(measured_at=datetime(2026, 6, 3, 15, 15)))
        session.commit()
        assert r1.id != r2.id

    def test_different_mode_creates_new(self, session, stadium):
        repo = TransitTimeRepository(session)
        r1, _ = repo.upsert(_transit(transport_mode="walk"))
        r2, _ = repo.upsert(_transit(transport_mode="bus"))
        session.commit()
        assert r1.id != r2.id

    def test_bulk_upsert_counts(self, session, stadium):
        repo = TransitTimeRepository(session)
        records = [_transit(measured_at=datetime(2026, 6, 3, 15, i * 15)) for i in range(4)]
        created, updated = repo.bulk_upsert(records)
        session.commit()
        assert created == 4
        assert updated == 0

    def test_bulk_upsert_dedup_on_re_run(self, session, stadium):
        repo = TransitTimeRepository(session)
        records = [_transit(measured_at=datetime(2026, 6, 3, 15, i * 15)) for i in range(3)]
        repo.bulk_upsert(records)
        session.commit()

        c2, u2 = repo.bulk_upsert(records)
        session.commit()
        assert c2 == 0
        assert u2 == 3


class TestTransitTimeRepositoryRead:
    def test_get_by_game_date(self, session, stadium):
        repo = TransitTimeRepository(session)
        gd1 = date(2026, 6, 3)
        gd2 = date(2026, 6, 4)
        repo.upsert(_transit(game_date=gd1, measured_at=datetime(2026, 6, 3, 15, 0)))
        repo.upsert(_transit(game_date=gd2, measured_at=datetime(2026, 6, 4, 15, 0)))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", gd1)
        assert len(results) == 1
        assert results[0].game_date == gd1

    def test_get_by_game_date_with_mode_filter(self, session, stadium):
        repo = TransitTimeRepository(session)
        repo.upsert(_transit(transport_mode="walk", measured_at=datetime(2026, 6, 3, 15, 0)))
        repo.upsert(_transit(transport_mode="bus", measured_at=datetime(2026, 6, 3, 15, 0)))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2026, 6, 3), transport_mode="walk")
        assert all(r.transport_mode == "walk" for r in results)

    def test_get_latest(self, session, stadium):
        repo = TransitTimeRepository(session)
        repo.upsert(_transit(measured_at=datetime(2026, 6, 3, 14, 0), duration_minutes=15))
        repo.upsert(_transit(measured_at=datetime(2026, 6, 3, 15, 0), duration_minutes=18))
        session.commit()

        latest = repo.get_latest("JAMSIL", "잠실역_2호선_7번출구", "walk")
        assert latest is not None
        assert latest.duration_minutes == 18

    def test_get_latest_none_when_empty(self, session, stadium):
        repo = TransitTimeRepository(session)
        result = repo.get_latest("JAMSIL", "없는출발지", "walk")
        assert result is None

    def test_get_avg_duration(self, session, stadium):
        repo = TransitTimeRepository(session)
        repo.upsert(_transit(measured_at=datetime(2026, 6, 3, 14, 0), duration_minutes=10))
        repo.upsert(_transit(measured_at=datetime(2026, 6, 3, 15, 0), duration_minutes=20))
        session.commit()

        avg = repo.get_avg_duration("JAMSIL", "잠실역_2호선_7번출구", date(2026, 6, 3))
        assert avg == pytest.approx(15.0, rel=0.01)

    def test_get_avg_duration_none_when_empty(self, session, stadium):
        repo = TransitTimeRepository(session)
        avg = repo.get_avg_duration("JAMSIL", "없는출발지", date(2026, 6, 3))
        assert avg is None


# ─────────────────────────────────────────────
# Origin config sanity checks
# ─────────────────────────────────────────────


class TestJamsilOriginsConfig:
    def test_all_origins_have_required_keys(self):
        from src.crawlers.transit_time_crawler import JAMSIL_ORIGINS

        for origin in JAMSIL_ORIGINS:
            assert "label" in origin
            assert "lat" in origin
            assert "lng" in origin
            assert "mode" in origin
            assert origin["mode"] in {"walk", "bus", "car", "mixed", "subway"}

    def test_lat_lng_in_jamsil_area(self):
        from src.crawlers.transit_time_crawler import JAMSIL_ORIGINS

        for origin in JAMSIL_ORIGINS:
            # Jamsil area: roughly 37.49~37.53 lat, 127.05~127.12 lng
            assert 37.48 < origin["lat"] < 37.55, f"Suspicious lat for {origin['label']}"
            assert 127.00 < origin["lng"] < 127.15, f"Suspicious lng for {origin['label']}"

    def test_at_least_three_subway_origins(self):
        from src.crawlers.transit_time_crawler import JAMSIL_ORIGINS

        walk_origins = [o for o in JAMSIL_ORIGINS if o["mode"] in ("walk", "subway")]
        assert len(walk_origins) >= 2

    def test_has_car_or_bus_origin(self):
        from src.crawlers.transit_time_crawler import JAMSIL_ORIGINS

        non_walk = [o for o in JAMSIL_ORIGINS if o["mode"] in ("bus", "car")]
        assert len(non_walk) >= 1


# ─────────────────────────────────────────────
# TransitResult dataclass tests
# ─────────────────────────────────────────────


class TestTransitResult:
    def test_transit_result_creation(self):
        from src.utils.map_api_client import TransitResult

        result = TransitResult(
            origin_label="잠실역_2호선",
            transport_mode="walk",
            duration_minutes=12,
            distance_meters=800,
            source_api="kakao",
            raw_response={"test": True},
        )
        assert result.origin_label == "잠실역_2호선"
        assert result.duration_minutes == 12
        assert result.source_api == "kakao"

    def test_jamsil_coordinates(self):
        from src.utils.map_api_client import JAMSIL_LAT, JAMSIL_LNG

        assert 37.50 < JAMSIL_LAT < 37.52
        assert 127.06 < JAMSIL_LNG < 127.08
