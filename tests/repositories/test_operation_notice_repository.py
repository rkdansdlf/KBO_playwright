from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.stadium_operation_notice import StadiumOperationNotice
from src.repositories.operation_notice_repository import OperationNoticeRepository


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    StadiumOperationNotice.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def _make_data(overrides=None) -> dict:
    base = {
        "stadium_code": "JAMSIL",
        "notice_type": "GATE_CHANGE",
        "title": "Gate 3 closed today",
        "content": "Gate 3 will be closed due to construction",
        "published_at": datetime(2024, 10, 15, 9, 0),
        "game_date": date(2024, 10, 15),
        "source_name": "LG트윈스공식",
        "source_url": "https://example.com/notice/1",
        "external_id": "ext-001",
        "is_urgent": True,
        "is_confirmed": True,
        "raw_snapshot": {"source": "crawl"},
    }
    if overrides:
        base.update(overrides)
    return base


class TestOperationNoticeRepository:
    def test_upsert_create(self, session):
        repo = OperationNoticeRepository(session)
        data = _make_data()
        record, created = repo.upsert(data)
        assert created
        assert record.title == "Gate 3 closed today"
        assert record.stadium_code == "JAMSIL"

    def test_upsert_update_by_external_id(self, session):
        repo = OperationNoticeRepository(session)
        repo.upsert(_make_data())
        session.commit()

        update_data = _make_data({"content": "Updated content", "is_urgent": False})
        record, created = repo.upsert(update_data)
        assert not created
        assert record.content == "Updated content"
        assert record.is_urgent is False

    def test_upsert_fallback_by_title_published_at(self, session):
        repo = OperationNoticeRepository(session)
        data = _make_data({"external_id": None})
        record, created = repo.upsert(data)
        assert created

        update_data = _make_data({"external_id": None, "content": "Fallback update"})
        record, created = repo.upsert(update_data)
        assert not created
        assert record.content == "Fallback update"

    def test_bulk_upsert_dedup(self, session):
        repo = OperationNoticeRepository(session)
        notices = [
            _make_data({"external_id": "a", "title": "A"}),
            _make_data({"external_id": "a", "title": "A"}),
            _make_data({"external_id": "b", "title": "B"}),
        ]
        created, updated = repo.bulk_upsert(notices)
        assert created == 2
        assert updated == 0

    def test_get_by_game_date(self, session):
        repo = OperationNoticeRepository(session)
        repo.upsert(_make_data({"game_date": date(2024, 10, 15), "external_id": "a", "title": "Notice A"}))
        repo.upsert(_make_data({"game_date": date(2024, 10, 15), "external_id": "b", "title": "Notice B"}))
        repo.upsert(_make_data({"game_date": date(2024, 10, 16), "external_id": "c", "title": "Notice C"}))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15))
        assert len(results) == 2

    def test_get_by_game_date_urgent_only(self, session):
        repo = OperationNoticeRepository(session)
        repo.upsert(_make_data({"is_urgent": True, "external_id": "u1", "title": "Urgent Notice"}))
        repo.upsert(_make_data({"is_urgent": False, "external_id": "n1", "title": "Normal Notice"}))
        session.commit()

        results = repo.get_by_game_date("JAMSIL", date(2024, 10, 15), urgent_only=True)
        assert len(results) == 1
        assert results[0].is_urgent is True

    def test_get_recent(self, session):
        repo = OperationNoticeRepository(session)
        for i in range(5):
            repo.upsert(_make_data({"external_id": f"e{i}", "title": f"Notice {i}"}))
        session.commit()

        results = repo.get_recent("JAMSIL", limit=3)
        assert len(results) == 3

    def test_get_recent_with_filters(self, session):
        repo = OperationNoticeRepository(session)
        repo.upsert(_make_data({"external_id": "a", "notice_type": "GATE_CHANGE", "source_name": "LG"}))
        repo.upsert(_make_data({"external_id": "b", "notice_type": "WEATHER", "source_name": "KBO"}))
        session.commit()

        results = repo.get_recent("JAMSIL", notice_type="WEATHER")
        assert len(results) == 1

    def test_get_urgent_today(self, session):
        repo = OperationNoticeRepository(session)
        today = date.today()
        repo.upsert(_make_data({"game_date": today, "is_urgent": True, "external_id": "u1"}))
        repo.upsert(_make_data({"game_date": today, "is_urgent": False, "external_id": "n1"}))
        session.commit()

        results = repo.get_urgent_today("JAMSIL")
        assert all(r.is_urgent for r in results)

    def test_get_latest_external_id(self, session):
        repo = OperationNoticeRepository(session)
        repo.upsert(
            _make_data(
                {
                    "external_id": "old-id",
                    "published_at": datetime(2024, 10, 14, 10, 0),
                }
            )
        )
        repo.upsert(
            _make_data(
                {
                    "external_id": "new-id",
                    "published_at": datetime(2024, 10, 15, 10, 0),
                }
            )
        )
        session.commit()

        latest = repo.get_latest_external_id("JAMSIL", "LG트윈스공식")
        assert latest == "new-id"

    def test_get_latest_external_id_none(self, session):
        repo = OperationNoticeRepository(session)
        repo.upsert(_make_data({"external_id": None, "title": "no-ext-id"}))
        session.commit()
        result = repo.get_latest_external_id("JAMSIL", "LG트윈스공식")
        assert result is None
