from __future__ import annotations

from datetime import date, datetime

import pytest

from src.utils.naver_helpers import (
    NAVER_TEAM_MAP,
    build_naver_sports_url,
    parse_iso_date,
    parse_multi_format_date,
)


class TestParseIsoDate:
    def test_none_returns_none(self):
        assert parse_iso_date(None) is None

    def test_empty_returns_none(self):
        assert parse_iso_date("") is None

    def test_iso_format(self):
        result = parse_iso_date("2025-06-15")
        assert result == date(2025, 6, 15)

    def test_iso_format_with_time(self):
        result = parse_iso_date("2025-06-15T14:30:00")
        assert result == date(2025, 6, 15)

    def test_invalid_returns_none(self):
        assert parse_iso_date("not-a-date") is None


class TestParseMultiFormatDate:
    def test_none_returns_none(self):
        assert parse_multi_format_date(None) is None

    def test_empty_returns_none(self):
        assert parse_multi_format_date("") is None

    def test_dot_format(self):
        result = parse_multi_format_date("2025.06.15")
        assert result is not None
        assert result.year == 2025
        assert result.month == 6
        assert result.day == 15
        assert result.tzinfo is not None

    def test_dash_format(self):
        result = parse_multi_format_date("2025-06-15")
        assert result is not None
        assert result.year == 2025

    def test_slash_format(self):
        result = parse_multi_format_date("2025/06/15")
        assert result is not None
        assert result.year == 2025

    def test_whitespace_stripped(self):
        result = parse_multi_format_date("  2025.06.15  ")
        assert result is not None

    def test_invalid_returns_none(self):
        assert parse_multi_format_date("not-a-date") is None


class TestBuildNaverSportsUrl:
    def test_with_oid_and_aid(self):
        url = build_naver_sports_url("123", "456")
        assert url == "https://sports.news.naver.com/kbaseball/news/read?oid=123&aid=456"

    def test_empty_oid_returns_empty(self):
        assert build_naver_sports_url("", "456") == ""

    def test_empty_aid_returns_empty(self):
        assert build_naver_sports_url("123", "") == ""

    def test_both_empty_returns_empty(self):
        assert build_naver_sports_url("", "") == ""


class TestNaverTeamMap:
    def test_all_teams_present(self):
        expected = {"LG", "KT", "NC", "DB", "LT", "SS", "KH", "HH", "KIA", "SSG"}
        assert set(NAVER_TEAM_MAP.values()) == expected

    def test_korean_to_code(self):
        assert NAVER_TEAM_MAP["롯데"] == "LT"
        assert NAVER_TEAM_MAP["삼성"] == "SS"
