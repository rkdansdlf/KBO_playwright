"""
Tests for StadiumCongestion repository and Seoul API client infrastructure.

Covers:
  - CongestionRepository upsert / bulk_upsert / dedup logic
  - get_by_game_date / get_latest / get_peak_congestion queries
  - Seoul API level mapping helper
  - CongestionSnapshot dataclass
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_congestion import StadiumCongestion
from src.models.stadium_info import StadiumInfo
from src.repositories.congestion_repository import CongestionRepository

# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    StadiumInfo.__table__.create(engine)
    StadiumCongestion.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


@pytest.fixture
def stadium(session):
    st = StadiumInfo(stadium_code="JAMSIL", name_kr="잠실야구장", home_team_id="LG")
    session.add(st)
    session.commit()
    return st


def _congestion(
    location_label="잠실역_2호선",
    location_type="subway_station",
    measured_at=None,
    game_date=None,
    congestion_level="normal",
    congestion_index=50.0,
    people_count=None,
    source="seoul_open_api",
):
    return {
        "stadium_code": "JAMSIL",
        "location_type": location_type,
        "location_label": location_label,
        "measured_at": measured_at or datetime(2026, 6, 3, 15, 0),
        "game_date": game_date or date(2026, 6, 3),
        "congestion_level": congestion_level,
        "congestion_index": congestion_index,
        "people_count": people_count,
        "source": source,
        "raw_data": {"test": True},
    }


# ─────────────────────────────────────────────
# CongestionRepository Tests
# ─────────────────────────────────────────────


class TestCongestionRepositoryUpsert:
    def test_insert_new_record(self, session, stadium):
        repo = CongestionRepository(session)
        rec, created = repo.upsert(_congestion())
        session.commit()
        assert created is True
        assert rec.id is not None
        assert rec.congestion_level == "normal"

    def test_dedup_by_composite_key(self, session, stadium):
        repo = CongestionRepository(session)
        data = _congestion(measured_at=datetime(2026, 6, 3, 15, 0))
        r1, c1 = repo.upsert(data)
        session.commit()

        # Same (stadium_code, location_label, measured_at) → update
        r2, c2 = repo.upsert({**data, "congestion_level": "high", "congestion_index": 75.0})
        session.commit()

        assert c1 is True
        assert c2 is False
        assert r1.id == r2.id
        assert r2.congestion_level == "high"
        assert r2.congestion_index == pytest.approx(75.0)

    def test_different_measured_at_creates_new(self, session, stadium):
        repo = CongestionRepository(session)
        r1, _ = repo.upsert(_congestion(measured_at=datetime(2026, 6, 3, 15, 0)))
        r2, _ = repo.upsert(_congestion(measured_at=datetime(2026, 6, 3, 15, 5)))
        session.commit()
        assert r1.id != r2.id

    def test_different_location_creates_new(self, session, stadium):
        repo = CongestionRepository(session)
        r1, _ = repo.upsert(_congestion(location_label="잠실역_2호선"))
        r2, _ = repo.upsert(_congestion(location_label="몽촌토성역_8호선"))
        session.commit()
        assert r1.id != r2.id

    def test_bulk_upsert_counts(self, session, stadium):
        repo = CongestionRepository(session)
        records = [
            _congestion(
                location_label=f"위치_{i}",
                measured_at=datetime(2026, 6, 3, 15, 0),
            )
            for i in range(4)
        ]
        created, updated = repo.bulk_upsert(records)
        session.commit()
        assert created == 4
        assert updated == 0

    def test_bulk_upsert_dedup_on_re_run(self, session, stadium):
        repo = CongestionRepository(session)
        records = [_congestion(location_label=f"위치_{i}", measured_at=datetime(2026, 6, 3, 15, 0)) for i in range(3)]
        repo.bulk_upsert(records)
        session.commit()

        c2, u2 = repo.bulk_upsert(records)
        session.commit()
        assert c2 == 0
        assert u2 == 3


class TestCongestionRepositoryRead:
    def test_get_by_game_date(self, session, stadium):
        repo = CongestionRepository(session)
        gd1 = date(2026, 6, 3)
        gd2 = date(2026, 6, 4)
        repo.upsert(_congestion(game_date=gd1, measured_at=datetime(2026, 6, 3, 15, 0)))
        repo.upsert(_congestion(game_date=gd2, measured_at=datetime(2026, 6, 4, 15, 0), location_label="위치B"))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", gd1)
        assert len(results) == 1
        assert results[0].game_date == gd1

    def test_get_by_game_date_location_type_filter(self, session, stadium):
        repo = CongestionRepository(session)
        repo.upsert(
            _congestion(location_type="gate", location_label="게이트1", measured_at=datetime(2026, 6, 3, 15, 0))
        )
        repo.upsert(
            _congestion(location_type="road", location_label="올림픽로", measured_at=datetime(2026, 6, 3, 15, 0))
        )
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2026, 6, 3), location_type="gate")
        assert all(r.location_type == "gate" for r in results)

    def test_get_latest(self, session, stadium):
        repo = CongestionRepository(session)
        repo.upsert(_congestion(measured_at=datetime(2026, 6, 3, 14, 0), congestion_level="low"))
        repo.upsert(_congestion(measured_at=datetime(2026, 6, 3, 15, 0), congestion_level="very_high"))
        session.commit()

        latest = repo.get_latest("JAMSIL", "잠실역_2호선")
        assert latest is not None
        assert latest.congestion_level == "very_high"

    def test_get_latest_none_when_empty(self, session, stadium):
        repo = CongestionRepository(session)
        result = repo.get_latest("JAMSIL", "없는위치")
        assert result is None

    def test_get_peak_congestion(self, session, stadium):
        repo = CongestionRepository(session)
        repo.upsert(
            _congestion(
                location_label="잠실역_2호선",
                measured_at=datetime(2026, 6, 3, 15, 0),
                congestion_index=50.0,
            )
        )
        repo.upsert(
            _congestion(
                location_label="잠실야구장_권역",
                measured_at=datetime(2026, 6, 3, 16, 0),
                congestion_index=92.0,
            )
        )
        session.commit()

        peak = repo.get_peak_congestion("JAMSIL", date(2026, 6, 3))
        assert peak is not None
        assert peak.congestion_index == pytest.approx(92.0)

    def test_get_peak_congestion_none_when_empty(self, session, stadium):
        repo = CongestionRepository(session)
        peak = repo.get_peak_congestion("JAMSIL", date(2099, 1, 1))
        assert peak is None


# ─────────────────────────────────────────────
# Seoul API level mapping tests
# ─────────────────────────────────────────────


class TestSeoulAPILevelMapping:
    def test_level_map_coverage(self):
        from src.utils.seoul_api_client import LEVEL_MAP

        expected_keys = {"여유", "보통", "약간 붐빔", "붐빔", "매우 붐빔"}
        assert expected_keys.issubset(set(LEVEL_MAP.keys()))

    def test_level_map_values(self):
        from src.utils.seoul_api_client import LEVEL_MAP

        assert LEVEL_MAP["여유"] == "low"
        assert LEVEL_MAP["보통"] == "normal"
        assert LEVEL_MAP["약간 붐빔"] == "high"
        assert LEVEL_MAP["붐빔"] == "very_high"

    def test_jamsil_area_codes_defined(self):
        from src.utils.seoul_api_client import JAMSIL_AREA_CODES

        assert len(JAMSIL_AREA_CODES) >= 2
        assert any("잠실" in code for code in JAMSIL_AREA_CODES)


# ─────────────────────────────────────────────
# CongestionSnapshot dataclass tests
# ─────────────────────────────────────────────


class TestCongestionSnapshot:
    def test_snapshot_creation(self):
        from src.utils.seoul_api_client import CongestionSnapshot

        snap = CongestionSnapshot(
            location_label="잠실역_2호선",
            congestion_level="high",
            congestion_index=75.0,
            people_count=12000,
            source="seoul_open_api",
            raw_data={"test": True},
        )
        assert snap.congestion_level == "high"
        assert snap.congestion_index == pytest.approx(75.0)
        assert snap.people_count == 12000

    def test_snapshot_optional_fields(self):
        from src.utils.seoul_api_client import CongestionSnapshot

        snap = CongestionSnapshot(
            location_label="잠실야구장_권역",
            congestion_level="normal",
            congestion_index=None,
            people_count=None,
            source="manual",
            raw_data={},
        )
        assert snap.congestion_index is None
        assert snap.people_count is None
