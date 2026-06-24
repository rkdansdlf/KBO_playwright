from __future__ import annotations

from src.utils.http_client import DEFAULT_HEADERS


class TestDefaultHeaders:
    def test_user_agent_present(self):
        assert "User-Agent" in DEFAULT_HEADERS

    def test_accept_language_present(self):
        assert "Accept-Language" in DEFAULT_HEADERS

    def test_user_agent_is_mozilla(self):
        assert "Mozilla/5.0" in DEFAULT_HEADERS["User-Agent"]

    def test_accept_language_includes_korean(self):
        assert "ko-KR" in DEFAULT_HEADERS["Accept-Language"]

    def test_two_headers(self):
        assert len(DEFAULT_HEADERS) == 2
