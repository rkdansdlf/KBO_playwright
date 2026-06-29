from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.crawlers.food_crawler as food_module
import src.crawlers.parking_crawler as parking_module
import src.crawlers.roster_transaction_crawler as roster_module
import src.crawlers.seat_crawler as seat_module
import src.crawlers.team_event_crawler as event_module
import src.crawlers.ticket_crawler as ticket_module
from src.models.source_registry import DataSource, RawSourceSnapshot
from src.repositories.source_registry_repository import DataSourceRepository, RawSourceSnapshotRepository


@pytest.fixture
def source_registry_session_factory():
    engine = create_engine("sqlite:///:memory:")
    DataSource.__table__.create(engine)
    RawSourceSnapshot.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


class _NoopRepository:
    def __init__(self, session):
        self.session = session

    def save(self, data):
        return SimpleNamespace(id=1)


def _seed_duplicate_snapshot(
    SessionLocal,
    *,
    source_key: str,
    target_domain: str,
    html: str,
    source_type: str = "official_team",
) -> str:
    content_hash = hashlib.sha256(html.encode()).hexdigest()
    with SessionLocal() as session:
        ds_repo = DataSourceRepository(session)
        ds = ds_repo.save(
            {
                "source_key": source_key,
                "source_type": source_type,
                "target_domain": target_domain,
                "is_active": True,
            },
        )
        session.flush()
        RawSourceSnapshotRepository(session).save(
            {
                "data_source_id": ds.id,
                "raw_html_or_json_path": f"https://example.com/{source_key}",
                "content_hash": content_hash,
                "fetched_at": datetime.now(UTC).replace(tzinfo=None),
                "status_code": 200,
            },
        )
        session.commit()
    return content_hash


def _assert_source_marked_success(SessionLocal, *, source_key: str, expected_hash: str) -> None:
    with SessionLocal() as session:
        ds = DataSourceRepository(session).get_by_key(source_key)
        assert ds is not None
        assert ds.last_success_at is not None
        assert ds.last_content_hash == expected_hash
        snapshots = RawSourceSnapshotRepository(session).get_by_source_id(ds.id)
        assert len(snapshots) == 1


def _set_duplicate_raw_page_and_save(crawler, source_key: str, html: str, save_call) -> None:
    crawler._raw_pages = [
        {
            "source_key": source_key,
            "url": f"https://example.com/{source_key}",
            "html": html,
            "status_code": 200,
        },
    ]
    save_call()


def test_team_event_save_marks_duplicate_raw_page_success(monkeypatch, source_registry_session_factory):
    source_key = "lg_twins_events"
    html = "<html>unchanged team event page</html>"
    expected_hash = _seed_duplicate_snapshot(
        source_registry_session_factory,
        source_key=source_key,
        target_domain="event",
        html=html,
    )
    monkeypatch.setattr(event_module, "SessionLocal", source_registry_session_factory)
    monkeypatch.setattr(event_module, "TeamEventRepository", _NoopRepository)

    crawler = event_module.TeamEventCrawler()
    _set_duplicate_raw_page_and_save(crawler, source_key, html, lambda: crawler._save_to_db([{"title": "event"}]))

    _assert_source_marked_success(source_registry_session_factory, source_key=source_key, expected_hash=expected_hash)


def test_roster_save_marks_duplicate_raw_page_success(monkeypatch, source_registry_session_factory):
    source_key = "kbo_today_roster"
    html = "<html>unchanged roster page</html>"
    expected_hash = _seed_duplicate_snapshot(
        source_registry_session_factory,
        source_key=source_key,
        target_domain="roster",
        html=html,
        source_type="official_kbo",
    )
    monkeypatch.setattr(roster_module, "SessionLocal", source_registry_session_factory)
    monkeypatch.setattr(roster_module, "RosterTransactionRepository", _NoopRepository)

    crawler = roster_module.RosterTransactionCrawler()
    data = [{"dedupe_key": "2026-06-07_LG_player_registered"}]
    _set_duplicate_raw_page_and_save(crawler, source_key, html, lambda: crawler._save_to_db(data))

    _assert_source_marked_success(source_registry_session_factory, source_key=source_key, expected_hash=expected_hash)


def test_ticket_save_marks_duplicate_raw_page_success(monkeypatch, source_registry_session_factory):
    source_key = "kbo_ticket_map"
    html = "<html>unchanged ticket page</html>"
    expected_hash = _seed_duplicate_snapshot(
        source_registry_session_factory,
        source_key=source_key,
        target_domain="ticket",
        html=html,
        source_type="official_kbo",
    )
    monkeypatch.setattr(ticket_module, "SessionLocal", source_registry_session_factory)
    monkeypatch.setattr(ticket_module, "TicketPriceRepository", _NoopRepository)
    monkeypatch.setattr(ticket_module, "TicketOpenRuleRepository", _NoopRepository)

    crawler = ticket_module.TicketCrawler()
    _set_duplicate_raw_page_and_save(crawler, source_key, html, lambda: crawler._save_to_db([{}], [{}]))

    _assert_source_marked_success(source_registry_session_factory, source_key=source_key, expected_hash=expected_hash)


def test_seat_save_marks_duplicate_raw_page_success(monkeypatch, source_registry_session_factory):
    source_key = "lg_twins_seat"
    html = "<html>unchanged seat page</html>"
    expected_hash = _seed_duplicate_snapshot(
        source_registry_session_factory,
        source_key=source_key,
        target_domain="seat",
        html=html,
    )
    monkeypatch.setattr(seat_module, "SessionLocal", source_registry_session_factory)
    monkeypatch.setattr(seat_module, "StadiumSeatSectionRepository", _NoopRepository)

    crawler = seat_module.SeatCrawler()
    _set_duplicate_raw_page_and_save(crawler, source_key, html, lambda: crawler._save_to_db([{}]))

    _assert_source_marked_success(source_registry_session_factory, source_key=source_key, expected_hash=expected_hash)


def test_parking_save_marks_duplicate_raw_page_success(monkeypatch, source_registry_session_factory):
    source_key = "ssg_landers_parking"
    html = "<html>unchanged parking page</html>"
    expected_hash = _seed_duplicate_snapshot(
        source_registry_session_factory,
        source_key=source_key,
        target_domain="parking",
        html=html,
    )
    monkeypatch.setattr(parking_module, "SessionLocal", source_registry_session_factory)
    monkeypatch.setattr(parking_module, "ParkingLotRepository", _NoopRepository)
    monkeypatch.setattr(parking_module, "ParkingFeeRuleRepository", _NoopRepository)

    crawler = parking_module.ParkingCrawler()
    data = [{"lot": {}, "fee_rules": [{}]}]
    _set_duplicate_raw_page_and_save(crawler, source_key, html, lambda: crawler._save_to_db(data))

    _assert_source_marked_success(source_registry_session_factory, source_key=source_key, expected_hash=expected_hash)


def test_food_save_marks_duplicate_raw_page_success(monkeypatch, source_registry_session_factory):
    source_key = "lotte_giants_fnb"
    html = "<html>unchanged food page</html>"
    expected_hash = _seed_duplicate_snapshot(
        source_registry_session_factory,
        source_key=source_key,
        target_domain="food",
        html=html,
    )
    monkeypatch.setattr(food_module, "SessionLocal", source_registry_session_factory)
    monkeypatch.setattr(food_module, "StadiumFoodVendorRepository", _NoopRepository)
    monkeypatch.setattr(food_module, "StadiumFoodMenuItemRepository", _NoopRepository)

    crawler = food_module.FoodCrawler()
    data = [{"vendor": {}, "menus": [{}]}]
    _set_duplicate_raw_page_and_save(crawler, source_key, html, lambda: crawler._save_to_db(data))

    _assert_source_marked_success(source_registry_session_factory, source_key=source_key, expected_hash=expected_hash)
