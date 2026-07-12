from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.parking_crawler import TEAM_PARKING_SOURCES, ParkingCrawler


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
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.get = AsyncMock(return_value=MagicMock(status_code=200, text="기본 요금 5,000원"))
        crawler = ParkingCrawler()
        info = TEAM_PARKING_SOURCES["LG"]

        with (
            patch("src.crawlers.parking_crawler.httpx.AsyncClient", return_value=client),
            patch("src.crawlers.parking_crawler.throttle.wait", new=AsyncMock()) as wait,
        ):
            lots = await crawler._crawl_team_parking("LG", info)

        wait.assert_awaited_once_with("stadium.seoul.go.kr")
        assert lots[0]["lot"]["stadium_id"] == "JAMSIL"
        assert crawler._raw_pages[0]["source_key"] == "jamsil_parking_official"

    @pytest.mark.asyncio
    async def test_run_continues_when_one_team_fails(self):
        crawler = ParkingCrawler()
        crawler._crawl_team_parking = AsyncMock(side_effect=[RuntimeError("LG unavailable"), [], []])

        records = await crawler.run()

        assert records == []
        assert crawler._crawl_team_parking.await_count == len(TEAM_PARKING_SOURCES)

    def test_save_to_db_persists_lot_and_fee_rules(self):
        session = MagicMock()
        lot_repo = MagicMock()
        lot_repo.save.return_value = MagicMock(id=11)
        fee_repo = MagicMock()
        crawler = ParkingCrawler()
        crawler._raw_pages = [{"source_key": "jamsil_parking_official"}]
        entry = {"lot": {"name": "잠실 주차장"}, "fee_rules": [{"label": "기본", "amount": 5000}]}

        with (
            patch("src.crawlers.parking_crawler.SessionLocal") as session_local,
            patch("src.crawlers.parking_crawler.save_raw_snapshots", return_value=1),
            patch("src.crawlers.parking_crawler.ParkingLotRepository", return_value=lot_repo),
            patch("src.crawlers.parking_crawler.ParkingFeeRuleRepository", return_value=fee_repo),
        ):
            session_local.return_value.__enter__.return_value = session
            crawler._save_to_db([entry])

        fee_repo.save.assert_called_once_with({"parking_lot_id": 11, "label": "기본", "amount": 5000})
        session.commit.assert_called_once()
        assert crawler._raw_pages == []
