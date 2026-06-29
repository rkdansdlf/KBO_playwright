from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.utils.naver_search_client import (
    NOTICE_QUERIES,
    NaverSearchClient,
    NaverSearchResult,
    _clean_html,
    _parse_naver_date,
)


class TestParseNaverDate:
    def test_none_returns_none(self):
        assert _parse_naver_date(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_naver_date("") is None

    def test_valid_rfc2822(self):
        result = _parse_naver_date("Tue, 03 Jun 2026 14:32:00 +0900")
        assert result is not None
        assert result.year == 2026
        assert result.month == 6
        assert result.day == 3

    def test_invalid_date_returns_none(self):
        assert _parse_naver_date("not a date at all") is None

    def test_fallback_date_format_dot(self):
        result = _parse_naver_date("2026.06.03.")
        assert result is not None
        assert result.year == 2026
        assert result.month == 6
        assert result.day == 3

    def test_fallback_date_format_dash(self):
        result = _parse_naver_date("2026-06-03")
        assert result is not None
        assert result.year == 2026
        assert result.month == 6
        assert result.day == 3

    def test_fallback_date_format_dot_no_trailing(self):
        result = _parse_naver_date("2026.06.03")
        assert result is not None
        assert result.year == 2026
        assert result.month == 6
        assert result.day == 3


class TestCleanHtml:
    def test_removes_b_tags(self):
        assert _clean_html("<b>title</b>") == "title"

    def test_removes_multiple_tags(self):
        assert _clean_html("<b>LG</b> <i>Twins</i>") == "LG Twins"

    def test_strips_whitespace(self):
        assert _clean_html("  hello  ") == "hello"

    def test_empty_string(self):
        assert _clean_html("") == ""

    def test_no_tags(self):
        assert _clean_html("plain text") == "plain text"


class TestNaverSearchResult:
    def test_naver_search_result_fields(self):
        result = NaverSearchResult(
            title="Test",
            description="Desc",
            link="http://example.com",
            pub_date=None,
            source_type="news",
            team_hint="LG",
            raw={},
        )
        assert result.title == "Test"
        assert result.team_hint == "LG"

    def test_naver_search_result_with_date(self):
        dt = datetime(2026, 6, 3, 14, 30)
        result = NaverSearchResult(
            title="News",
            description="Desc",
            link="http://naver.com",
            pub_date=dt,
            source_type="blog",
            team_hint=None,
            raw={},
        )
        assert result.pub_date == dt
        assert result.source_type == "blog"


class TestNoticeQueries:
    def test_notice_queries_not_empty(self):
        assert len(NOTICE_QUERIES) > 0

    def test_each_query_has_required_keys(self):
        for q in NOTICE_QUERIES:
            assert "query" in q
            assert "notice_types" in q
            assert isinstance(q["notice_types"], list)


def _make_search_response(items=None):
    if items is None:
        items = [
            {
                "title": "<b>LG</b> 입장 안내",
                "description": "오늘 <b>잠실</b> 경기",
                "link": "https://naver.com/1",
                "originallink": "https://example.com/1",
                "pubDate": "Tue, 03 Jun 2026 14:32:00 +0900",
            },
            {
                "title": "두산 공지",
                "description": "우천 연기 안내",
                "link": "https://naver.com/2",
                "originallink": "https://example.com/2",
                "pubDate": "Mon, 02 Jun 2026 10:00:00 +0900",
            },
        ]
    return {"items": items}


@pytest.mark.asyncio
class TestNaverSearchClient:
    async def test_not_configured_returns_empty(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        client = NaverSearchClient()
        results = await client.search("LG Twins")
        assert results == []

    async def test_http_error_returns_empty(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        client = NaverSearchClient()

        import httpx

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(side_effect=httpx.HTTPError("connection failed"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search("LG Twins")

        assert results == []

    async def test_search_success(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = _make_search_response()

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search("LG Twins", search_type="news", display=10)

        assert len(results) == 2
        assert results[0].title == "LG 입장 안내"
        assert results[0].description == "오늘 잠실 경기"
        assert results[0].link == "https://naver.com/1"
        assert results[0].source_type == "news"
        assert results[0].pub_date is not None
        assert results[1].title == "두산 공지"

    async def test_search_with_empty_items(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"items": []}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search("query")

        assert results == []

    async def test_search_with_no_items_key(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {}

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search("query")

        assert results == []

    async def test_search_uses_originallink_when_link_empty(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        response_data = {
            "items": [
                {
                    "title": "Test",
                    "description": "Desc",
                    "link": "",
                    "originallink": "https://original.com",
                    "pubDate": "Tue, 03 Jun 2026 14:32:00 +0900",
                },
            ],
        }

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search("query")

        assert len(results) == 1
        assert results[0].link == "https://original.com"

    async def test_search_with_invalid_pub_date(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        response_data = {
            "items": [
                {
                    "title": "Test",
                    "description": "Desc",
                    "link": "https://test.com",
                    "pubDate": "invalid date",
                },
            ],
        }

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            results = await client.search("query")

        assert len(results) == 1
        assert results[0].pub_date is None

    @pytest.mark.slow
    async def test_search_kbo_notices_without_keys(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        client = NaverSearchClient()
        results = await client.search_kbo_notices(days_back=3)
        assert results == []

    async def test_search_kbo_notices_success(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        response_data = _make_search_response(
            items=[
                {
                    "title": "<b>LG</b> 입장 안내",
                    "description": "공지",
                    "link": f"https://naver.com/{i}",
                    "originallink": f"https://example.com/{i}",
                    "pubDate": "Tue, 03 Jun 2026 14:32:00 +0900",
                }
                for i in range(2)
            ],
        )

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        from src.constants import KST

        def mock_parse_date(date_str):
            from datetime import datetime

            if not date_str:
                return None
            return datetime(2026, 6, 3, tzinfo=KST)

        with patch("httpx.AsyncClient", return_value=mock_client):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.utils.naver_search_client._parse_naver_date", mock_parse_date):
                    results = await client.search_kbo_notices(days_back=30)

        assert len(results) > 0
        assert all(r.team_hint is not None for r in results)

    async def test_search_kbo_notices_deduplicates_links(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        response_data = _make_search_response(
            items=[
                {
                    "title": "Same Article",
                    "description": "Desc",
                    "link": "https://naver.com/same",
                    "originallink": "https://example.com/same",
                    "pubDate": "Tue, 03 Jun 2026 14:32:00 +0900",
                },
            ],
        )

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        from src.constants import KST

        def mock_parse_date(date_str):
            from datetime import datetime

            if not date_str:
                return None
            return datetime(2026, 6, 3, tzinfo=KST)

        with patch("httpx.AsyncClient", return_value=mock_client):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.utils.naver_search_client._parse_naver_date", mock_parse_date):
                    results = await client.search_kbo_notices(days_back=30)

        links = [r.link for r in results]
        assert len(links) == len(set(links))

    async def test_search_kbo_notices_filters_old_results(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test-secret")
        client = NaverSearchClient()

        response_data = _make_search_response(
            items=[
                {
                    "title": "Old Article",
                    "description": "Old",
                    "link": "https://naver.com/old",
                    "originallink": "https://example.com/old",
                    "pubDate": "Mon, 01 Jan 2024 00:00:00 +0900",
                },
            ],
        )

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = response_data

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.get = AsyncMock(return_value=mock_response)

        from src.constants import KST

        def mock_parse_date(date_str):
            from datetime import datetime

            if not date_str:
                return None
            return datetime(2024, 1, 1, tzinfo=KST)

        with patch("httpx.AsyncClient", return_value=mock_client):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with patch("src.utils.naver_search_client._parse_naver_date", mock_parse_date):
                    results = await client.search_kbo_notices(days_back=3)

        assert results == []


class TestNaverSearchClientSync:
    def test_is_configured(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        client = NaverSearchClient()
        assert client._is_configured() is True

    def test_is_configured_false(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        client = NaverSearchClient()
        assert client._is_configured() is False

    def test_is_configured_partial_id_only(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        client = NaverSearchClient()
        assert client._is_configured() is False

    def test_is_configured_partial_secret_only(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "secret")
        client = NaverSearchClient()
        assert client._is_configured() is False

    def test_headers(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "my-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "my-secret")
        client = NaverSearchClient()
        headers = client._headers()
        assert headers["X-Naver-Client-Id"] == "my-id"
        assert headers["X-Naver-Client-Secret"] == "my-secret"
