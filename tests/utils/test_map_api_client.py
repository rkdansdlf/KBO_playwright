from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from src.utils.map_api_client import (
    TransitRequest,
    TransitResult,
    _call_kakao,
    _call_naver,
    _call_tmap,
    get_transit_time,
    get_transit_times_batch,
)


class TestTransitResult:
    def test_transit_result_fields(self):
        result = TransitResult(
            origin_label="Jamsil",
            transport_mode="subway",
            duration_minutes=30,
            distance_meters=5000,
            source_api="kakao",
            raw_response={},
        )
        assert result.origin_label == "Jamsil"
        assert result.duration_minutes == 30


@pytest.mark.asyncio
class TestCallKakao:
    async def test_missing_key_returns_none(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "")
        client = AsyncMock()
        result = await _call_kakao(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None

    async def test_http_error_returns_none(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "test-key")
        client = AsyncMock()
        client.get.side_effect = httpx.HTTPError("HTTP error")
        result = await _call_kakao(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None


@pytest.mark.asyncio
class TestCallNaver:
    async def test_missing_credentials_returns_none(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        client = AsyncMock()
        result = await _call_naver(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None

    async def test_http_error_returns_none(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        client = AsyncMock()
        client.get.side_effect = httpx.HTTPError("HTTP error")
        result = await _call_naver(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None


@pytest.mark.asyncio
class TestCallTmap:
    async def test_missing_key_returns_none(self, monkeypatch):
        monkeypatch.setenv("TMAP_API_KEY", "")
        client = AsyncMock()
        result = await _call_tmap(client, 37.0, 127.0, 37.5, 127.1, "mixed")
        assert result is None

    async def test_http_error_returns_none(self, monkeypatch):
        monkeypatch.setenv("TMAP_API_KEY", "tmap-key")
        client = AsyncMock()
        client.post.side_effect = httpx.HTTPError("HTTP error")
        result = await _call_tmap(client, 37.0, 127.0, 37.5, 127.1, "mixed")
        assert result is None


@pytest.mark.asyncio
class TestGetTransitTime:
    async def test_no_keys_returns_none(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "")
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        monkeypatch.setenv("TMAP_API_KEY", "")
        result = await get_transit_time(TransitRequest("test", 37.0, 127.0))
        assert result is None


@pytest.mark.asyncio
class TestGetTransitTimesBatch:
    async def test_empty_origins_returns_empty(self, monkeypatch):
        monkeypatch.setenv("KAKAO_REST_API_KEY", "")
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        monkeypatch.setenv("TMAP_API_KEY", "")
        result = await get_transit_times_batch([])
        assert result == []

    async def test_batch_returns_transit_results(self, monkeypatch):
        async def mock_get_transit_time(req):
            return TransitResult(
                origin_label=req.origin_label,
                transport_mode=req.mode,
                duration_minutes=30,
                distance_meters=5000,
                source_api="kakao",
                raw_response={},
            )

        monkeypatch.setattr("src.utils.map_api_client.get_transit_time", mock_get_transit_time)

        origins = [{"label": "A", "lat": 37.0, "lng": 127.0}]
        result = await get_transit_times_batch(origins)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].origin_label == "A"


class TestCallKakaoEdge:
    @pytest.mark.asyncio
    async def test_nonzero_result_code_returns_none(self, monkeypatch):
        from src.utils.map_api_client import _call_kakao

        client = AsyncMock()
        client.get.side_effect = [
            MagicMock(json=lambda: {"routes": [{"result_code": 1}], "raise_for_status": lambda: None}),
        ]
        result = await _call_kakao(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None


class TestCallNaverEdge:
    @pytest.mark.asyncio
    async def test_json_error_returns_none(self, monkeypatch):
        from src.utils.map_api_client import _call_naver

        client = AsyncMock()
        mock_resp = MagicMock()
        mock_resp.json.side_effect = ValueError("bad json")
        client.get.return_value = mock_resp
        result = await _call_naver(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None


class TestCallTmapEdge:
    @pytest.mark.asyncio
    async def test_nonok_result_code_returns_none(self, monkeypatch):
        from src.utils.map_api_client import _call_tmap

        client = AsyncMock()
        client.post.return_value = {"resultCode": "FAIL"}
        monkeypatch.setenv("TMAP_REST_API_KEY", "key")
        result = await _call_tmap(client, 37.0, 127.0, 37.5, 127.1, "car")
        assert result is None


class TestGetTransitTimeEdge:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_keys(self, monkeypatch):
        from src.utils.map_api_client import get_transit_time, TransitRequest

        monkeypatch.setenv("KAKAO_REST_API_KEY", "")
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        monkeypatch.setenv("TMAP_REST_API_KEY", "")
        result = await get_transit_time(
            TransitRequest(
                origin_label="A",
                origin_lat=37.0,
                origin_lng=127.0,
                dest_lat=37.5,
                dest_lng=127.1,
            ),
        )
        assert result is None
