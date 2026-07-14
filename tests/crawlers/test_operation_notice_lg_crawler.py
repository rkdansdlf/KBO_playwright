from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.constants import KST

from datetime import datetime

import httpx
import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.operation_notice_common import classify_notice as _classify_notice, is_urgent as _is_urgent
from src.crawlers.operation_notice_lg_crawler import (
    OperationNoticeLGCrawler,
    _extract_article_id,
    _parse_date,
)


class TestClassifyNotice:
    def test_cancel(self):
        assert _classify_notice("우천 취소 안내") == "CANCEL"
        assert _classify_notice("노게임 공지") == "CANCEL"

    def test_delay(self):
        assert _classify_notice("경기 지연") == "DELAY"
        assert _classify_notice("입장 딜레이") == "DELAY"

    def test_gate_change(self):
        assert _classify_notice("게이트 변경 안내") == "GATE_CHANGE"
        assert _classify_notice("입장문 안내") == "GATE_CHANGE"

    def test_entry_rule(self):
        assert _classify_notice("반입 금지 물품") == "ENTRY_RULE"
        assert _classify_notice("입장 제한") == "ENTRY_RULE"

    def test_parking(self):
        assert _classify_notice("주차장 안내") == "PARKING"

    def test_weather(self):
        assert _classify_notice("태풍 주의") == "WEATHER"
        assert _classify_notice("기상 상황") == "WEATHER"

    def test_general(self):
        assert _classify_notice("일반 공지") == "GENERAL"
        assert _classify_notice("") == "GENERAL"


class TestIsUrgent:
    def test_urgent(self):
        assert _is_urgent("[긴급] 경기 취소")
        assert _is_urgent("[필독] 공지")
        assert _is_urgent("[중요] 안내")
        assert _is_urgent("긴급공지")
        assert _is_urgent("즉시 확인")

    def test_not_urgent(self):
        assert not _is_urgent("일반 공지")
        assert not _is_urgent("")
        assert not _is_urgent("주차 안내")


class TestParseDate:
    def test_dot_format(self):
        result = _parse_date("2026.06.03")
        assert result == datetime(2026, 6, 3, tzinfo=KST)

    def test_dash_format(self):
        result = _parse_date("2026-06-03")
        assert result == datetime(2026, 6, 3, tzinfo=KST)

    def test_slash_format(self):
        result = _parse_date("2026/06/03")
        assert result == datetime(2026, 6, 3, tzinfo=KST)

    def test_invalid_date(self):
        assert _parse_date("") is None
        assert _parse_date("not-a-date") is None

    def test_with_whitespace(self):
        result = _parse_date("  2026.06.03  ")
        assert result == datetime(2026, 6, 3, tzinfo=KST)


class TestExtractArticleId:
    def test_idx_param(self):
        assert _extract_article_id("/notice?idx=12345") == "12345"

    def test_id_param(self):
        assert _extract_article_id("/board?id=67890") == "67890"

    def test_trailing_digits(self):
        assert _extract_article_id("/notice/54321") == "54321"

    def test_no_match(self):
        assert _extract_article_id("/notice/about") is None
        assert _extract_article_id("") is None


class TestParsePage:
    def setup_method(self):
        self.crawler = OperationNoticeLGCrawler()

    def test_single_notice(self):
        html = """
        <ul class="news_list">
            <li>
                <a href="/service/announcement?idx=1001">우천 취소 안내</a>
                <span class="date">2026.06.03</span>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, None)
        assert len(notices) == 1
        assert notices[0]["title"] == "우천 취소 안내"
        assert notices[0]["notice_type"] == "CANCEL"
        assert notices[0]["external_id"] == "1001"
        assert notices[0]["stadium_code"] == "JAMSIL"
        assert notices[0]["source_name"] == "LG트윈스공식"
        assert notices[0]["is_confirmed"] is True
        assert hit_stop is False

    def test_multiple_notices_with_stop(self):
        html = """
        <ul class="news_list">
            <li>
                <a href="?idx=1003">[긴급] 게이트 변경</a>
                <span class="date">2026.06.01</span>
            </li>
            <li>
                <a href="?idx=1002">주차 변경 안내</a>
                <span class="date">2026.06.02</span>
            </li>
            <li>
                <a href="?idx=1001">우천 취소</a>
                <span class="date">2026.06.03</span>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, "1001")
        assert len(notices) == 2
        assert hit_stop is True

    def test_empty_html(self):
        notices, hit_stop = self.crawler._parse_page("<html></html>", None)
        assert notices == []
        assert hit_stop is False

    def test_title_fallback(self):
        html = """
        <ul class="news_list">
            <li>
                <a href="?idx=2001">공지사항</a>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, None)
        assert len(notices) == 1
        assert notices[0]["title"] == "공지사항"
        assert notices[0]["external_id"] == "2001"
        assert hit_stop is False

    def test_url_prefix_added(self):
        html = """
        <ul class="news_list">
            <li>
                <a href="/service/notice?idx=3001">테스트 공지</a>
                <span class="date">2026.06.03</span>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, None)
        assert len(notices) == 1
        assert "lgtwins.com" in notices[0]["source_url"]

    def test_strips_session_id_and_skips_incomplete_rows(self):
        html = """
        <ul class="news_list">
            <li>링크 없음</li>
            <li><a href="/announcement;jsessionid=abc.123?idx=4001"></a></li>
            <li><a href="/announcement;jsessionid=abc.123?idx=4002">날씨 안내</a></li>
        </ul>
        """

        notices, hit_stop = self.crawler._parse_page(html, None)

        assert hit_stop is False
        assert len(notices) == 1
        assert notices[0]["source_url"] == "https://www.lgtwins.com/announcement?idx=4002"
        assert notices[0]["published_at"] is None
        assert notices[0]["notice_type"] == "WEATHER"


class TestOperationNoticeLGCrawlerRun:
    @staticmethod
    def _client(response_or_error):
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=None)
        client.get = AsyncMock(return_value=response_or_error)
        return client

    @pytest.mark.asyncio
    async def test_run_collects_notices_records_raw_page_and_calls_save(self):
        html = """
        <ul class="news_list">
            <li><a href="?idx=5001">[긴급] 경기 취소</a><span class="date">2026.06.03</span></li>
        </ul>
        """
        client = self._client(MagicMock(status_code=200, text=html))
        crawler = OperationNoticeLGCrawler(max_pages=1)
        crawler._save_to_db = MagicMock()

        with (
            patch("src.crawlers.operation_notice_lg_crawler.httpx.AsyncClient", return_value=client),
            patch("src.crawlers.operation_notice_lg_crawler.throttle.wait", new=AsyncMock()) as wait,
        ):
            notices = await crawler.run(save=True)

        assert [notice["external_id"] for notice in notices] == ["5001"]
        assert crawler._raw_pages == [
            {
                "source_key": "lg_twins_notices",
                "url": "https://www.lgtwins.com/twins/feed/news?page=1",
                "html": html,
                "status_code": 200,
            },
        ]
        wait.assert_awaited_once_with("www.lgtwins.com")
        crawler._save_to_db.assert_called_once_with(notices)

    @pytest.mark.asyncio
    async def test_run_stops_on_http_error_or_non_success_response(self):
        crawler = OperationNoticeLGCrawler(max_pages=2)
        client = self._client(MagicMock(status_code=503, text=""))

        with (
            patch("src.crawlers.operation_notice_lg_crawler.httpx.AsyncClient", return_value=client),
            patch("src.crawlers.operation_notice_lg_crawler.throttle.wait", new=AsyncMock()),
        ):
            assert await crawler.run() == []

        client.get.side_effect = httpx.HTTPError("unavailable")
        with (
            patch("src.crawlers.operation_notice_lg_crawler.httpx.AsyncClient", return_value=client),
            patch("src.crawlers.operation_notice_lg_crawler.throttle.wait", new=AsyncMock()),
        ):
            assert await crawler.run() == []

    def test_save_to_db_commits_and_clears_raw_pages(self):
        crawler = OperationNoticeLGCrawler()
        crawler._raw_pages = [{"url": "https://example.test"}]
        session = MagicMock()
        repository = MagicMock()
        repository.bulk_upsert.return_value = (2, 1)

        with (
            patch("src.crawlers.operation_notice_lg_crawler.SessionLocal") as session_local,
            patch("src.crawlers.operation_notice_lg_crawler.OperationNoticeRepository", return_value=repository),
        ):
            session_local.return_value.__enter__.return_value = session
            crawler._save_to_db([{"external_id": "1"}])

        repository.bulk_upsert.assert_called_once_with([{"external_id": "1"}])
        session.commit.assert_called_once()
        assert crawler._raw_pages == []

    def test_save_to_db_rolls_back_database_error(self):
        crawler = OperationNoticeLGCrawler()
        session = MagicMock()
        repository = MagicMock()
        repository.bulk_upsert.side_effect = SQLAlchemyError("write failed")

        with (
            patch("src.crawlers.operation_notice_lg_crawler.SessionLocal") as session_local,
            patch("src.crawlers.operation_notice_lg_crawler.OperationNoticeRepository", return_value=repository),
        ):
            session_local.return_value.__enter__.return_value = session
            crawler._save_to_db([{"external_id": "1"}])

        session.rollback.assert_called_once()
