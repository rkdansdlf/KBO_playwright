from __future__ import annotations
from src.constants import KST

from datetime import datetime

from src.crawlers.operation_notice_common import classify_notice as _classify, is_urgent as _is_urgent
from src.crawlers.operation_notice_naver_crawler import (
    _infer_game_date,
    _result_to_notice,
)
from src.utils.naver_search_client import NaverSearchResult


class TestClassify:
    def test_cancel(self):
        assert _classify("오늘 경기 취소") == "CANCEL"
        assert _classify("우천 취소 안내") == "CANCEL"

    def test_delay(self):
        assert _classify("경기 지연 공지") == "DELAY"
        assert _classify("딜레이 발생") == "DELAY"

    def test_gate_change(self):
        assert _classify("게이트 변경") == "GATE_CHANGE"
        assert _classify("입장문 안내") == "GATE_CHANGE"

    def test_entry_rule(self):
        assert _classify("반입 금지 물품") == "ENTRY_RULE"
        assert _classify("입장 제한 안내") == "ENTRY_RULE"

    def test_parking(self):
        assert _classify("주차장 혼잡") == "PARKING"

    def test_weather(self):
        assert _classify("태풍 주의") == "WEATHER"
        assert _classify("오늘 날씨 안내") == "WEATHER"

    def test_general(self):
        assert _classify("일반 공지사항입니다") == "GENERAL"
        assert _classify("") == "GENERAL"


class TestIsUrgent:
    def test_urgent_keywords(self):
        assert _is_urgent("[긴급] 경기 취소")
        assert _is_urgent("[필독] 공지사항")
        assert _is_urgent("[중요] 입장 변경")
        assert _is_urgent("긴급 공지")
        assert _is_urgent("경기 취소 안내")  # "취소" matches via 긴급? No — let me check
        assert _is_urgent("긴급공지")

    def test_not_urgent(self):
        assert not _is_urgent("일반 공지")
        assert not _is_urgent("")
        assert not _is_urgent("주차 안내")


class TestInferGameDate:
    def test_with_pub_date(self):
        result = _infer_game_date(
            NaverSearchResult(
                title="test",
                description="",
                link="",
                pub_date=datetime(2026, 6, 3, 14, 30),
                source_type="news",
                team_hint="LG",
                raw={},
            )
        )
        assert result == datetime(2026, 6, 3, tzinfo=KST).date()

    def test_no_pub_date(self):
        result = _infer_game_date(
            NaverSearchResult(
                title="test",
                description="",
                link="",
                pub_date=None,
                source_type="news",
                team_hint=None,
                raw={},
            )
        )
        assert result is not None  # falls back to today


class TestResultToNotice:
    def test_full_conversion(self):
        result = NaverSearchResult(
            title="[긴급] LG vs 두산 경기 우천 취소",
            description="오늘 경기가 우천으로 취소되었습니다.",
            link="https://example.com/news/123",
            pub_date=datetime(2026, 6, 3, 10, 0),
            source_type="news",
            team_hint="LG",
            raw={},
        )
        notice = _result_to_notice(result)
        assert notice["stadium_code"] == "JAMSIL"
        assert notice["notice_type"] == "CANCEL"
        assert notice["is_urgent"] is True
        assert notice["title"] == "[긴급] LG vs 두산 경기 우천 취소"
        assert notice["content"] == "오늘 경기가 우천으로 취소되었습니다."
        assert notice["source_name"] == "naver_search_LG"
        assert notice["source_url"] == "https://example.com/news/123"
        assert notice["external_id"] == "https://example.com/news/123"
        assert notice["published_at"] == datetime(2026, 6, 3, 10, 0)
        assert notice["game_date"] == datetime(2026, 6, 3, tzinfo=KST).date()
        assert notice["is_confirmed"] is False
        assert notice["raw_snapshot"] == {}

    def test_team_hint_none(self):
        result = NaverSearchResult(
            title="잠실 경기 공지",
            description="",
            link="https://example.com/news/456",
            pub_date=None,
            source_type="news",
            team_hint=None,
            raw={},
        )
        notice = _result_to_notice(result)
        assert notice["source_name"] == "naver_search_잠실"
