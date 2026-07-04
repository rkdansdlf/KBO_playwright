from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.utils.seoul_api_client import (
    CongestionSnapshot,
    get_area_congestion,
    get_jamsil_congestion_batch,
)


def _make_response(json_data: dict) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = json_data
    return resp


class TestGetAreaCongestionAlternatePaths:
    @pytest.mark.asyncio
    async def test_alternate_citydata_key(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _AltKeyClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response(
                    {
                        "SeoulRtd.citydata_ppltn": {"RESULT": "not_a_list"},
                        "CITYDATA": {
                            "LIVE_PPLTN_STTS": [
                                {
                                    "AREA_CONGEST_LVL": "붐빔",
                                    "AREA_PPLTN_MAX": "20000",
                                    "AREA_PPLTN_MIN": "15000",
                                },
                            ],
                        },
                    },
                )

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _AltKeyClient())
        result = await get_area_congestion("잠실역(2호선)")
        assert result is not None
        assert result.congestion_level == "very_high"
        assert result.people_count == 17500

    @pytest.mark.asyncio
    async def test_invalid_people_count_values(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _BadPpltnClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response(
                    {
                        "SeoulRtd.citydata_ppltn": {
                            "RESULT": [
                                {
                                    "AREA_CONGEST_LVL": "여유",
                                    "AREA_PPLTN_MAX": "not_a_number",
                                    "AREA_PPLTN_MIN": "also_not",
                                },
                            ],
                        },
                    },
                )

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _BadPpltnClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is not None
        assert result.people_count is None

    @pytest.mark.asyncio
    async def test_missing_people_count_fields(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _NoPpltnClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response(
                    {
                        "SeoulRtd.citydata_ppltn": {
                            "RESULT": [
                                {
                                    "AREA_CONGEST_LVL": "보통",
                                },
                            ],
                        },
                    },
                )

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _NoPpltnClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is not None
        assert result.people_count is None
        assert result.congestion_index == 50.0

    @pytest.mark.asyncio
    async def test_unknown_congestion_level_defaults_normal(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _UnknownLevelClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response(
                    {
                        "SeoulRtd.citydata_ppltn": {
                            "RESULT": [
                                {
                                    "AREA_CONGEST_LVL": "알수없음",
                                },
                            ],
                        },
                    },
                )

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _UnknownLevelClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is not None
        assert result.congestion_level == "normal"

    @pytest.mark.asyncio
    async def test_unexpected_error_returns_none(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _ErrorClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                resp = MagicMock()
                resp.raise_for_status = MagicMock()
                resp.json.return_value = {"unexpected": True}
                return resp

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _ErrorClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is None

    @pytest.mark.asyncio
    async def test_empty_citydata_list_returns_none(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")

        class _EmptyListClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                return _make_response(
                    {
                        "SeoulRtd.citydata_ppltn": {"RESULT": "not_a_list"},
                        "CITYDATA": {"LIVE_PPLTN_STTS": []},
                    },
                )

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _EmptyListClient())
        result = await get_area_congestion("잠실 야구장")
        assert result is None


class TestGetJamsilCongestionBatchMixed:
    @pytest.mark.asyncio
    async def test_filters_out_exceptions(self, monkeypatch):
        monkeypatch.setenv("SEOUL_OPEN_DATA_API_KEY", "test-key")
        call_count = 0

        class _MixedClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 2:
                    raise Exception("timeout")
                return _make_response(
                    {
                        "SeoulRtd.citydata_ppltn": {
                            "RESULT": [
                                {"AREA_CONGEST_LVL": "여유"},
                            ],
                        },
                    },
                )

        monkeypatch.setattr("httpx.AsyncClient", lambda **kwargs: _MixedClient())
        results = await get_jamsil_congestion_batch()
        assert len(results) < 3
        assert all(isinstance(r, CongestionSnapshot) for r in results)
