from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.parking_crawler import TEAM_PARKING_SOURCES, ParkingCrawler
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun


@pytest.fixture(autouse=True)
def ledger_sessions(monkeypatch):
    """원장은 이제 ``run()``의 필수 의존성이므로 테스트용 DB를 연결한다."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", factory)


class TestParseParkingPage:
    def setup_method(self):
        self.crawler = ParkingCrawler()

    def test_parses_parking_fees(self):
        html = "<html><body>기본 요금: 5,000원 추가 1,000원</body></html>"
        info = {"stadium_id": "MUNHAK"}
        result = self.crawler._parse_parking_page(html, info)
        assert len(result) == 1
        assert result[0]["lot"]["stadium_id"] == "MUNHAK"
        fees = result[0]["fee_rules"]
        assert len(fees) >= 1
        assert any(f["label"] == "기본" for f in fees)

    def test_no_fees_still_returns_lot(self):
        html = "<html><body>주차장 정보만 있습니다.</body></html>"
        info = {"stadium_id": "DAEGU"}
        result = self.crawler._parse_parking_page(html, info)
        assert len(result) == 1
        assert result[0]["fee_rules"] == []

    def test_lot_metadata(self):
        html = "<html><body>주차 가능</body></html>"
        info = {"stadium_id": "JAMSIL"}
        result = self.crawler._parse_parking_page(html, info)
        assert result[0]["lot"]["lot_type"] == "official"
        assert result[0]["lot"]["is_event_day_available"] is True


def test_parking_sources_cover_seeded_jamsil_source():
    assert TEAM_PARKING_SOURCES["LG"]["source_key"] == "jamsil_parking_official"
    assert TEAM_PARKING_SOURCES["LG"]["stadium_id"] == "JAMSIL"


class TestParkingCrawlerOperations:
    @pytest.mark.asyncio
    async def test_crawl_team_fetches_lots_and_tracks_snapshot(self):
        crawler = ParkingCrawler()
        crawler._http.fetch_text = AsyncMock(
            return_value=CrawlResult.success("기본 요금 5,000원", http_status=200, url="https://example.invalid"),
        )

        lots = await crawler._crawl_team_parking("LG", TEAM_PARKING_SOURCES["LG"])

        assert lots[0]["lot"]["stadium_id"] == "JAMSIL"
        assert crawler._raw_pages[0]["source_key"] == "jamsil_parking_official"

    @pytest.mark.asyncio
    async def test_crawl_team_returns_empty_for_failed_fetch(self):
        crawler = ParkingCrawler()
        crawler._http.fetch_text = AsyncMock(
            return_value=CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error="HTTP 404",
                http_status=404,
            ),
        )

        lots = await crawler._crawl_team_parking("LG", TEAM_PARKING_SOURCES["LG"])

        assert lots == []
        assert crawler._raw_pages == []

    @pytest.mark.asyncio
    async def test_run_continues_when_one_team_fails(self):
        crawler = ParkingCrawler()
        crawler._crawl_team_parking = AsyncMock(side_effect=[RuntimeError("LG unavailable"), [], []])

        records = await crawler.run()

        assert records == []
        assert crawler._crawl_team_parking.await_count == len(TEAM_PARKING_SOURCES)

    def test_save_to_db_persists_lots_and_leaves_the_fee_text_to_the_snapshot(self):
        """Lots are written; the parsed fee rules deliberately are not.

        The pages state fees by kind (기본/추가/일일/행사) while
        ``parking_fee_rules`` is keyed by vehicle class (compact/sedan/van/bus)
        and requires a non-null base duration. Neither column can be filled from
        what the page says, so filling them would mean inventing them -- and
        writing the kinds into ``vehicle_type`` would leave a fabricated vehicle
        class in a column a real one later keys on. The text survives in the raw
        snapshot, which is committed before these rows and can be re-parsed by
        ``kbo snapshot replay`` when a schema that fits it exists.
        """
        session = MagicMock()
        lot_repo = MagicMock()
        lot_repo.save.return_value = MagicMock(id=11)
        crawler = ParkingCrawler()
        crawler._raw_pages = [{"source_key": "jamsil_parking_official"}]
        entry = {
            "team_code": "OB",
            "lot": {"name": "잠실 주차장"},
            "fee_rules": [{"label": "기본", "amount": 5000}],
        }

        with (
            patch("src.crawlers.parking_crawler.SessionLocal") as session_local,
            patch("src.crawlers.parking_crawler.save_raw_snapshots", return_value=1),
            patch("src.crawlers.parking_crawler.ParkingLotRepository", return_value=lot_repo),
        ):
            session_local.return_value.__enter__.return_value = session
            crawler._save_to_db([entry])

        lot_repo.save.assert_called_once_with({"name": "잠실 주차장"})
        # Two commits: snapshots first, so a failed lot write still leaves the
        # page a replay can re-parse.
        assert session.commit.call_count == 2
        assert crawler._raw_pages == []
