import pytest

from src.utils.seoul_api_client import (
    JAMSIL_AREA_CODES,
    LEVEL_MAP,
    CongestionSnapshot,
    get_area_congestion,
    get_jamsil_congestion_batch,
)


class TestCongestionSnapshot:
    def test_snapshot_fields(self):
        snap = CongestionSnapshot(
            location_label="Jamsil",
            congestion_level="high",
            congestion_index=75.0,
            people_count=500,
            source="seoul_open_api",
            raw_data={},
        )
        assert snap.location_label == "Jamsil"
        assert snap.congestion_level == "high"
        assert snap.congestion_index == 75.0


class TestLevelMap:
    def test_level_map_contains_all(self):
        assert LEVEL_MAP["여유"] == "low"
        assert LEVEL_MAP["보통"] == "normal"
        assert LEVEL_MAP["약간 붐빔"] == "high"
        assert LEVEL_MAP["붐빔"] == "very_high"
        assert LEVEL_MAP["매우 붐빔"] == "very_high"


class TestJamsilAreaCodes:
    def test_jamsil_area_codes_defined(self):
        assert "잠실 야구장" in JAMSIL_AREA_CODES
        assert "잠실역(2호선)" in JAMSIL_AREA_CODES
        assert len(JAMSIL_AREA_CODES) == 3


@pytest.mark.asyncio
class TestGetAreaCongestion:
    async def test_missing_key_returns_none(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "")
        result = await get_area_congestion("잠실 야구장")
        assert result is None

    async def test_http_error_returns_none(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")
        result = await get_area_congestion("잠실 야구장")
        assert result is None


@pytest.mark.asyncio
class TestGetJamsilCongestionBatch:
    async def test_no_key_returns_empty(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "")
        result = await get_jamsil_congestion_batch()
        assert result == []

    async def test_with_key_returns_list(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")
        result = await get_jamsil_congestion_batch()
        assert isinstance(result, list)
