from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.utils.map_api_client import (
    TransitRequest,
    TransitResult,
    _call_kakao,
    _call_naver,
    _call_tmap,
    get_transit_time,
)


def _make_resp(json_data: dict, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    resp.status_code = status
    return resp


@pytest.mark.asyncio
class TestKakaoSuccessPath:
    async def test_successful_response_returns_payload(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "test-key")
        client = AsyncMock()
        client.get.return_value = _make_resp(
            {
                "routes": [
                    {
                        "result_code": 0,
                        "summary": {"duration": 1800, "distance": 4500},
                    },
                ],
            },
        )
        result = await _call_kakao(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is not None
        assert result["duration_seconds"] == 1800
        assert result["distance_meters"] == 4500
        assert "raw" in result

    async def test_empty_routes_returns_none(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "test-key")
        client = AsyncMock()
        client.get.return_value = _make_resp({"routes": []})
        result = await _call_kakao(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None

    async def test_nonzero_result_code_skips_route(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "test-key")
        client = AsyncMock()
        client.get.return_value = _make_resp(
            {
                "routes": [{"result_code": 1, "summary": {"duration": 100}}],
            },
        )
        result = await _call_kakao(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None


@pytest.mark.asyncio
class TestNaverSuccessPath:
    async def test_successful_response_returns_payload(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        client = AsyncMock()
        client.get.return_value = _make_resp(
            {
                "route": {
                    "trafast": [
                        {
                            "summary": {"duration": 1800000, "distance": 6000},
                        },
                    ],
                },
            },
        )
        result = await _call_naver(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is not None
        assert result["duration_seconds"] == 1800
        assert result["distance_meters"] == 6000
        assert "raw" in result

    async def test_missing_route_returns_none(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        client = AsyncMock()
        client.get.return_value = _make_resp({"route": None})
        result = await _call_naver(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None

    async def test_empty_trafast_list_returns_none(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        client = AsyncMock()
        client.get.return_value = _make_resp({"route": {"trafast": []}})
        result = await _call_naver(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None


@pytest.mark.asyncio
class TestTmapSuccessPath:
    async def test_successful_response_returns_payload(self, monkeypatch):
        monkeypatch.setenv("TMAP_API_KEY", "tmap-key")
        client = AsyncMock()
        client.post.return_value = _make_resp(
            {
                "metaData": {
                    "plan": {
                        "itineraries": [
                            {
                                "totalTime": 2400,
                                "totalDistance": 7200,
                            },
                        ],
                    },
                },
            },
        )
        result = await _call_tmap(client, 37.0, 127.0, 37.5, 127.1, "mixed")
        assert result is not None
        assert result["duration_seconds"] == 2400
        assert result["distance_meters"] == 7200
        assert "raw" in result

    async def test_missing_itineraries_returns_none(self, monkeypatch):
        monkeypatch.setenv("TMAP_API_KEY", "tmap-key")
        client = AsyncMock()
        client.post.return_value = _make_resp({"metaData": {"plan": {}}})
        result = await _call_tmap(client, 37.0, 127.0, 37.5, 127.1, "mixed")
        assert result is None

    async def test_empty_itineraries_returns_none(self, monkeypatch):
        monkeypatch.setenv("TMAP_API_KEY", "tmap-key")
        client = AsyncMock()
        client.post.return_value = _make_resp({"metaData": {"plan": {"itineraries": []}}})
        result = await _call_tmap(client, 37.0, 127.0, 37.5, 127.1, "mixed")
        assert result is None


@pytest.mark.asyncio
class TestGetTransitTimeSuccess:
    async def test_kakao_success_returns_transit_result(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "key")
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        monkeypatch.setenv("TMAP_API_KEY", "")

        mock_resp = _make_resp(
            {
                "routes": [
                    {
                        "result_code": 0,
                        "summary": {"duration": 1800, "distance": 5000},
                    },
                ],
            },
        )

        with patch("httpx.AsyncClient", autospec=True) as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.get.return_value = mock_resp
            result = await get_transit_time(TransitRequest("Jamsil", 37.0, 127.0))

        assert isinstance(result, TransitResult)
        assert result.origin_label == "Jamsil"
        assert result.duration_minutes == 30
        assert result.distance_meters == 5000
        assert result.source_api == "kakao"

    async def test_naver_fallback_returns_transit_result(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "")
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        monkeypatch.setenv("TMAP_API_KEY", "")

        mock_resp = _make_resp(
            {
                "route": {
                    "trafast": [
                        {
                            "summary": {"duration": 1200000, "distance": 3000},
                        },
                    ],
                },
            },
        )

        with patch("httpx.AsyncClient", autospec=True) as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.get.return_value = mock_resp
            result = await get_transit_time(TransitRequest("Gangnam", 37.0, 127.0))

        assert isinstance(result, TransitResult)
        assert result.duration_minutes == 20
        assert result.source_api == "naver"

    async def test_tmap_fallback_returns_transit_result(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "")
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        monkeypatch.setenv("TMAP_API_KEY", "tmap-key")

        mock_resp = _make_resp(
            {
                "metaData": {
                    "plan": {
                        "itineraries": [
                            {
                                "totalTime": 3600,
                                "totalDistance": 8000,
                            },
                        ],
                    },
                },
            },
        )

        with patch("httpx.AsyncClient", autospec=True) as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.post.return_value = mock_resp
            result = await get_transit_time(TransitRequest("Hongdae", 37.0, 127.0))

        assert isinstance(result, TransitResult)
        assert result.duration_minutes == 60
        assert result.source_api == "tmap"

    async def test_duration_minimum_one_minute(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "key")
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        monkeypatch.setenv("TMAP_API_KEY", "")

        mock_resp = _make_resp(
            {
                "routes": [
                    {
                        "result_code": 0,
                        "summary": {"duration": 10, "distance": 100},
                    },
                ],
            },
        )

        with patch("httpx.AsyncClient", autospec=True) as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.get.return_value = mock_resp
            result = await get_transit_time(TransitRequest("Nearby", 37.0, 127.0))

        assert result is not None
        assert result.duration_minutes == 1

    async def test_all_apis_fail_returns_none(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "key")
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        monkeypatch.setenv("TMAP_API_KEY", "tmap-key")

        mock_resp = _make_resp({"routes": []})

        with patch("httpx.AsyncClient", autospec=True) as MockClient:
            instance = MockClient.return_value.__aenter__.return_value
            instance.get.return_value = mock_resp
            instance.post.return_value = _make_resp({"metaData": {"plan": {}}})
            result = await get_transit_time(TransitRequest("Nowhere", 37.0, 127.0))

        assert result is None
