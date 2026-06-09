"""Tests for http_client — default HTTP headers."""

from src.utils.http_client import DEFAULT_HEADERS


class TestDefaultHeaders:
    def test_has_user_agent(self):
        assert "User-Agent" in DEFAULT_HEADERS
        assert "Mozilla" in DEFAULT_HEADERS["User-Agent"]

    def test_has_accept_language(self):
        assert "Accept-Language" in DEFAULT_HEADERS
        assert "ko-KR" in DEFAULT_HEADERS["Accept-Language"]
