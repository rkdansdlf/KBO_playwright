
import pytest

from src.utils.naver_search_client import (
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


class TestCleanHtml:
    def test_removes_b_tags(self):
        assert _clean_html("<b>title</b>") == "title"

    def test_removes_multiple_tags(self):
        assert _clean_html("<b>LG</b> <i>Twins</i>") == "LG Twins"

    def test_strips_whitespace(self):
        assert _clean_html("  hello  ") == "hello"


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
        results = await client.search("LG Twins")
        assert isinstance(results, list)

    async def test_search_kbo_notices_without_keys(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "")
        client = NaverSearchClient()
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

    def test_headers(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "my-id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "my-secret")
        client = NaverSearchClient()
        headers = client._headers()
        assert headers["X-Naver-Client-Id"] == "my-id"
        assert headers["X-Naver-Client-Secret"] == "my-secret"
