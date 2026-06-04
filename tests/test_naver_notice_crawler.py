"""
Tests for Naver Search-based operation notice crawler.

Covers:
  - NaverSearchClient configuration check
  - _parse_naver_date with multiple formats
  - _clean_html tag removal
  - _classify notice type detection
  - _is_urgent detection
  - _result_to_notice payload construction
  - OperationNoticeNaverCrawler skips when unconfigured
  - CLI --source argument parsing
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.crawlers.operation_notice_naver_crawler import (
    _classify,
    _infer_game_date,
    _is_urgent,
    _result_to_notice,
)
from src.utils.naver_search_client import (
    NOTICE_QUERIES,
    NaverSearchClient,
    NaverSearchResult,
    _clean_html,
    _parse_naver_date,
)


# ─────────────────────────────────────────────
# naver_search_client helpers
# ─────────────────────────────────────────────

class TestParseNaverDate:
    def test_rfc2822_format(self):
        d = _parse_naver_date("Tue, 03 Jun 2026 14:32:00 +0900")
        assert d is not None
        assert d.year == 2026
        assert d.month == 6
        assert d.day == 3

    def test_dot_format(self):
        d = _parse_naver_date("2026.06.03.")
        assert d is not None
        assert d.year == 2026 and d.month == 6

    def test_dash_format(self):
        d = _parse_naver_date("2026-06-03")
        assert d is not None
        assert d.year == 2026 and d.day == 3

    def test_empty_string_returns_none(self):
        assert _parse_naver_date("") is None

    def test_garbage_returns_none(self):
        assert _parse_naver_date("invalid-date-xyz") is None


class TestCleanHtml:
    def test_removes_bold_tags(self):
        assert _clean_html("<b>LG트윈스</b> 공지") == "LG트윈스 공지"

    def test_no_tags_unchanged(self):
        assert _clean_html("잠실야구장 입장") == "잠실야구장 입장"

    def test_removes_anchor_tags(self):
        assert _clean_html('<a href="#">링크</a>') == "링크"

    def test_empty_string(self):
        assert _clean_html("") == ""


class TestNaverSearchClientConfig:
    def test_not_configured_without_env(self, monkeypatch):
        monkeypatch.delenv("NAVER_CLIENT_ID", raising=False)
        monkeypatch.delenv("NAVER_CLIENT_SECRET", raising=False)
        client = NaverSearchClient()
        assert client._is_configured() is False

    def test_configured_with_env(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "test_id")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test_secret")
        client = NaverSearchClient()
        assert client._is_configured() is True

    def test_headers_contain_credentials(self, monkeypatch):
        monkeypatch.setenv("NAVER_CLIENT_ID", "myid")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "mysecret")
        client = NaverSearchClient()
        headers = client._headers()
        assert headers["X-Naver-Client-Id"] == "myid"
        assert headers["X-Naver-Client-Secret"] == "mysecret"


class TestNoticeQueries:
    def test_queries_defined(self):
        assert len(NOTICE_QUERIES) >= 3

    def test_all_queries_have_required_fields(self):
        for q in NOTICE_QUERIES:
            assert "query" in q
            assert "team" in q
            assert "notice_types" in q

    def test_lg_query_exists(self):
        lg_queries = [q for q in NOTICE_QUERIES if q.get("team") == "LG"]
        assert len(lg_queries) >= 1

    def test_doosan_query_exists(self):
        ob_queries = [q for q in NOTICE_QUERIES if q.get("team") == "OB"]
        assert len(ob_queries) >= 1


# ─────────────────────────────────────────────
# operation_notice_naver_crawler helpers
# ─────────────────────────────────────────────

class TestClassify:
    def test_cancel(self):
        assert _classify("경기 우천 취소 안내") == "CANCEL"

    def test_cancel_nogame(self):
        assert _classify("노게임 처리 안내") == "CANCEL"

    def test_gate_change(self):
        assert _classify("게이트 변경 공지") == "GATE_CHANGE"

    def test_entry_rule(self):
        assert _classify("입장 규정 안내") == "ENTRY_RULE"

    def test_parking(self):
        assert _classify("주차 안내 및 주차장 변경") == "PARKING"

    def test_weather(self):
        assert _classify("태풍 경보 발령으로 인한 안내") == "WEATHER"

    def test_shuttle_entry(self):
        assert _classify("셔틀버스 운행 안내") == "ENTRY_RULE"

    def test_general(self):
        assert _classify("이번 주말 특별 이벤트 안내") == "GENERAL"


class TestIsUrgent:
    def test_urgent_bracket(self):
        assert _is_urgent("[긴급] 경기 취소") is True

    def test_urgent_filok(self):
        assert _is_urgent("[필독] 오늘 경기 안내") is True

    def test_urgent_cancel_keyword(self):
        assert _is_urgent("오늘 취소 공지") is True

    def test_not_urgent(self):
        assert _is_urgent("다음 경기 일정 안내") is False


class TestResultToNotice:
    def _make_result(self, title="LG트윈스 게이트 변경", team_hint="LG"):
        return NaverSearchResult(
            title=title,
            description="상세 내용입니다.",
            link="https://n.news.naver.com/article/1234",
            pub_date=datetime(2026, 6, 3, 14, 0),
            source_type="news",
            team_hint=team_hint,
            raw={"title": title},
        )

    def test_stadium_code_is_jamsil(self):
        notice = _result_to_notice(self._make_result())
        assert notice["stadium_code"] == "JAMSIL"

    def test_external_id_is_url(self):
        result = self._make_result()
        notice = _result_to_notice(result)
        assert notice["external_id"] == result.link

    def test_is_confirmed_false(self):
        notice = _result_to_notice(self._make_result())
        assert notice["is_confirmed"] is False

    def test_notice_type_classified(self):
        notice = _result_to_notice(self._make_result(title="우천 취소 공지"))
        assert notice["notice_type"] == "CANCEL"

    def test_game_date_from_pub_date(self):
        notice = _result_to_notice(self._make_result())
        assert notice["game_date"] == date(2026, 6, 3)

    def test_source_name_lg(self):
        notice = _result_to_notice(self._make_result(team_hint="LG"))
        assert "LG" in notice["source_name"]

    def test_source_name_doosan(self):
        notice = _result_to_notice(self._make_result(team_hint="OB"))
        assert "두산" in notice["source_name"]

    def test_title_truncated_to_500(self):
        long_title = "A" * 600
        notice = _result_to_notice(self._make_result(title=long_title))
        assert len(notice["title"]) <= 500


# ─────────────────────────────────────────────
# OperationNoticeNaverCrawler integration
# ─────────────────────────────────────────────

class TestNaverCrawlerIntegration:
    def test_returns_empty_when_not_configured(self, monkeypatch):
        import asyncio
        from src.crawlers.operation_notice_naver_crawler import OperationNoticeNaverCrawler
        monkeypatch.delenv("NAVER_CLIENT_ID", raising=False)
        monkeypatch.delenv("NAVER_CLIENT_SECRET", raising=False)
        crawler = OperationNoticeNaverCrawler()
        result = asyncio.run(crawler.run(save=False))
        assert result == []

    def test_uses_search_client_results(self, monkeypatch):
        import asyncio
        from src.crawlers.operation_notice_naver_crawler import OperationNoticeNaverCrawler

        fake_result = NaverSearchResult(
            title="LG트윈스 우천 취소 공지",
            description="오늘 경기 우천 취소",
            link="https://n.news.naver.com/999",
            pub_date=datetime(2026, 6, 3, 12, 0),
            source_type="news",
            team_hint="LG",
            raw={},
        )

        async def fake_search_kbo_notices(days_back=3):
            return [fake_result]

        monkeypatch.setenv("NAVER_CLIENT_ID", "test")
        monkeypatch.setenv("NAVER_CLIENT_SECRET", "test")

        crawler = OperationNoticeNaverCrawler()
        crawler.client.search_kbo_notices = fake_search_kbo_notices

        notices = asyncio.run(crawler.run(save=False))
        assert len(notices) == 1
        assert notices[0]["notice_type"] == "CANCEL"
        assert notices[0]["is_confirmed"] is False


# ─────────────────────────────────────────────
# CLI argument parsing
# ─────────────────────────────────────────────

class TestCLIArgumentParsing:
    def test_default_source_is_official(self):
        from src.cli.crawl_operation_notices import build_arg_parser
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.source == "official"

    def test_source_naver(self):
        from src.cli.crawl_operation_notices import build_arg_parser
        parser = build_arg_parser()
        args = parser.parse_args(["--source", "naver"])
        assert args.source == "naver"

    def test_source_all(self):
        from src.cli.crawl_operation_notices import build_arg_parser
        parser = build_arg_parser()
        args = parser.parse_args(["--source", "all", "--save"])
        assert args.source == "all"
        assert args.save is True

    def test_days_default(self):
        from src.cli.crawl_operation_notices import build_arg_parser
        parser = build_arg_parser()
        args = parser.parse_args(["--source", "naver"])
        assert args.days == 3

    def test_days_custom(self):
        from src.cli.crawl_operation_notices import build_arg_parser
        parser = build_arg_parser()
        args = parser.parse_args(["--source", "naver", "--days", "1"])
        assert args.days == 1

    def test_team_and_pages(self):
        from src.cli.crawl_operation_notices import build_arg_parser
        parser = build_arg_parser()
        args = parser.parse_args(["--team", "LG", "--pages", "3", "--save"])
        assert args.team == "LG"
        assert args.pages == 3
