from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.seoul_api_client import (
    LEVEL_MAP,
    CongestionSnapshot,
    get_area_congestion,
    get_jamsil_congestion_batch,
)


class TestLevelMap:
    def test_all_levels_mapped(self) -> None:
        assert LEVEL_MAP["여유"] == "low"
        assert LEVEL_MAP["보통"] == "normal"
        assert LEVEL_MAP["약간 붐빔"] == "high"
        assert LEVEL_MAP["붐빔"] == "very_high"
        assert LEVEL_MAP["매우 붐빔"] == "very_high"


def _make_response(json_data: dict) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = json_data
    return resp


class TestGetAreaCongestion:
    @pytest.mark.asyncio
    async def test_returns_none_without_api_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("SEOUL_OPEN_DATA_API_KEY", raising=False)
        result = await get_area_congestion("잠실 야구장")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_http_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _FailClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                import httpx
                raise httpx.HTTPError("connection failed")

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _FailClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_empty_result(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _EmptyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response({})

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _EmptyClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is None

    @pytest.mark.asyncio
    async def test_parses_congestion_snapshot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _OkClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response({
                    "SeoulRtd.citydata_ppltn": {
                        "RESULT": [
                            {
                                "AREA_CONGEST_LVL": "보통",
                                "AREA_PPLTN_MAX": "5000",
                                "AREA_PPLTN_MIN": "3000",
                            }
                        ]
                    }
                })

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _OkClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is not None
        assert isinstance(result, CongestionSnapshot)
        assert result.location_label == "잠실 야구장"
        assert result.congestion_level == "normal"
        assert result.people_count == 4000
        assert result.congestion_index == 50.0
        assert result.source == "seoul_open_api"


class TestGetJamsilCongestionBatch:
    @pytest.mark.asyncio
    async def test_returns_snapshots(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _OkClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response({
                    "SeoulRtd.citydata_ppltn": {
                        "RESULT": [
                            {
                                "AREA_CONGEST_LVL": "여유",
                                "AREA_PPLTN_MAX": "100",
                                "AREA_PPLTN_MIN": "50",
                            }
                        ]
                    }
                })

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _OkClient())
        results = await get_jamsil_congestion_batch()
        assert len(results) == 3
        assert all(isinstance(r, CongestionSnapshot) for r in results)
