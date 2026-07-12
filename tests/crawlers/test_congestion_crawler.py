from __future__ import annotations

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.crawlers.congestion_crawler import CongestionCrawler, _snapshot_to_record


class FakeSnapshot:
    def __init__(self, location_label, congestion_level, congestion_index, people_count, source, raw_data):
        self.location_label = location_label
        self.congestion_level = congestion_level
        self.congestion_index = congestion_index
        self.people_count = people_count
        self.source = source
        self.raw_data = raw_data


class TestSnapshotToRecord:
    def test_known_location(self):
        snap = FakeSnapshot(
            location_label="잠실 야구장",
            congestion_level="보통",
            congestion_index=50,
            people_count=1000,
            source="seoul_api",
            raw_data='{"some": "data"}',
        )
        result = _snapshot_to_record(snap, date(2025, 5, 1), datetime(2025, 5, 1, 10, 0))
        assert result["stadium_code"] == "JAMSIL"
        assert result["location_type"] == "area"
        assert result["location_label"] == "잠실야구장_권역"
        assert result["congestion_level"] == "보통"
        assert result["congestion_index"] == 50

    def test_unknown_location(self):
        snap = FakeSnapshot("새로운 장소", "혼잡", 80, 500, "seoul_api", None)
        result = _snapshot_to_record(snap, date(2025, 5, 1), datetime(2025, 5, 1, 10, 0))
        assert result["location_label"] == "새로운 장소"
        assert result["location_type"] == "area"

    def test_game_date_and_measured_at(self):
        snap = FakeSnapshot("잠실역(2호선)", "여유", 20, 200, "seoul_api", None)
        gdate = date(2025, 6, 15)
        measured = datetime(2025, 6, 15, 12, 30)
        result = _snapshot_to_record(snap, gdate, measured)
        assert result["game_date"] == gdate
        assert result["measured_at"] == measured
        assert result["location_type"] == "subway_station"


class TestCongestionCrawler:
    @pytest.mark.asyncio
    async def test_run_collects_records_without_saving(self):
        snapshot = FakeSnapshot("잠실 야구장", "보통", 50, 1000, "seoul_api", {"level": "보통"})
        crawler = CongestionCrawler()

        with patch(
            "src.crawlers.congestion_crawler.get_jamsil_congestion_batch",
            new=AsyncMock(return_value=[snapshot]),
        ) as collect:
            records = await crawler.run(game_date=date(2025, 5, 1))

        collect.assert_awaited_once()
        assert len(records) == 1
        assert records[0]["game_date"] == date(2025, 5, 1)
        assert records[0]["location_label"] == "잠실야구장_권역"

    @pytest.mark.asyncio
    async def test_collect_seoul_api_returns_empty_for_http_error(self):
        crawler = CongestionCrawler()

        with patch(
            "src.crawlers.congestion_crawler.get_jamsil_congestion_batch",
            new=AsyncMock(side_effect=httpx.HTTPError("unavailable")),
        ):
            records = await crawler._collect_seoul_api()

        assert records == []

    def test_save_to_db_commits_upserts(self):
        session = MagicMock()
        repository = MagicMock()
        repository.bulk_upsert.return_value = (2, 1)
        records = [{"location_label": "잠실야구장_권역"}]

        with (
            patch("src.crawlers.congestion_crawler.SessionLocal") as session_local,
            patch("src.crawlers.congestion_crawler.CongestionRepository", return_value=repository),
        ):
            session_local.return_value.__enter__.return_value = session
            CongestionCrawler()._save_to_db(records)

        repository.bulk_upsert.assert_called_once_with(records)
        session.commit.assert_called_once()

    def test_save_to_db_rolls_back_database_error(self):
        session = MagicMock()
        repository = MagicMock()
        repository.bulk_upsert.side_effect = __import__("sqlalchemy").exc.SQLAlchemyError("write failed")

        with (
            patch("src.crawlers.congestion_crawler.SessionLocal") as session_local,
            patch("src.crawlers.congestion_crawler.CongestionRepository", return_value=repository),
        ):
            session_local.return_value.__enter__.return_value = session
            CongestionCrawler()._save_to_db([{}])

        session.rollback.assert_called_once()
