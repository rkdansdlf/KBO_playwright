from __future__ import annotations
from src.constants import KST

from datetime import datetime

from src.crawlers.operation_notice_doosan_crawler import (
    OperationNoticeDoosanCrawler,
    _classify_notice,
    _extract_article_id,
    _is_urgent,
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
        assert _parse_date("06.03") is None

    def test_with_whitespace(self):
        result = _parse_date("  2026.06.03  ")
        assert result == datetime(2026, 6, 3, tzinfo=KST)


class TestExtractArticleId:
    def test_idx_param(self):
        assert _extract_article_id("/notice?idx=12345") == "12345"

    def test_id_param(self):
        assert _extract_article_id("/board?id=67890") == "67890"

    def test_seq_param(self):
        assert _extract_article_id("/board?seq=1111") == "1111"

    def test_trailing_digits(self):
        assert _extract_article_id("/notice/54321") == "54321"

    def test_no_match(self):
        assert _extract_article_id("/notice/about") is None
        assert _extract_article_id("") is None


class TestParsePage:
    def setup_method(self):
        self.crawler = OperationNoticeDoosanCrawler()

    def test_single_notice(self):
        html = """
        <ul class="list-box">
            <li>
                <a href="/notice?idx=1001">우천 취소 안내</a>
                <p class="txt">2026.06.03</p>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, None)
        assert len(notices) == 1
        assert notices[0]["title"] == "우천 취소 안내"
        assert notices[0]["notice_type"] == "CANCEL"
        assert notices[0]["external_id"] == "1001"
        assert notices[0]["stadium_code"] == "JAMSIL"
        assert notices[0]["source_name"] == "두산베어스공식"
        assert notices[0]["is_confirmed"] is True
        assert hit_stop is False

    def test_multiple_notices(self):
        html = """
        <ul class="list-box">
            <li>
                <a href="/notice?idx=1001">우천 취소 안내</a>
                <p class="txt">2026.06.03</p>
            </li>
            <li>
                <a href="/notice?idx=1002">주차장 변경</a>
                <p class="txt">2026.06.02</p>
            </li>
            <li>
                <a href="/notice?idx=1003">[긴급] 게이트 변경</a>
                <p class="txt">2026.06.01</p>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, None)
        assert len(notices) == 3
        assert notices[0]["notice_type"] == "CANCEL"
        assert notices[1]["notice_type"] == "PARKING"
        assert notices[2]["notice_type"] == "GATE_CHANGE"
        assert notices[2]["is_urgent"] is True

    def test_stop_at_external_id(self):
        html = """
        <ul class="list-box">
            <li>
                <a href="/notice?idx=1002">일반 공지</a>
                <p class="txt">2026.06.02</p>
            </li>
            <li>
                <a href="/notice?idx=1001">우천 취소</a>
                <p class="txt">2026.06.03</p>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, "1001")
        assert len(notices) == 1
        assert hit_stop is True

    def test_empty_html(self):
        notices, hit_stop = self.crawler._parse_page("<html></html>", None)
        assert notices == []
        assert hit_stop is False

    def test_url_relative_resolution(self):
        html = """
        <ul class="list-box">
            <li>
                <a href="./notice/123">일반 공지</a>
                <p class="txt">2026.06.03</p>
            </li>
        </ul>
        """
        notices, hit_stop = self.crawler._parse_page(html, None)
        assert len(notices) == 1
        assert "doosanbears.com" in notices[0]["source_url"]
