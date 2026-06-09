import pytest
from datetime import date, datetime

from src.crawlers.congestion_crawler import _snapshot_to_record


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
