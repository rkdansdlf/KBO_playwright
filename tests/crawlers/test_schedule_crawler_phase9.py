from __future__ import annotations

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DELAYED,
    GAME_STATUS_LIVE,
    GAME_STATUS_POSTPONED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_SUSPENDED,
)


class TestNormalizeScheduleStatus:
    def test_none_returns_scheduled(self):
        assert ScheduleCrawler._normalize_schedule_status(None) == GAME_STATUS_SCHEDULED

    def test_empty_string_returns_scheduled(self):
        assert ScheduleCrawler._normalize_schedule_status("") == GAME_STATUS_SCHEDULED

    def test_whitespace_returns_scheduled(self):
        assert ScheduleCrawler._normalize_schedule_status("   ") == GAME_STATUS_SCHEDULED

    def test_completed_variants(self):
        assert ScheduleCrawler._normalize_schedule_status("경기종료") == GAME_STATUS_COMPLETED
        assert ScheduleCrawler._normalize_schedule_status("종료") == GAME_STATUS_COMPLETED

    def test_live_variants(self):
        assert ScheduleCrawler._normalize_schedule_status("경기중") == GAME_STATUS_LIVE
        assert ScheduleCrawler._normalize_schedule_status("진행중") == GAME_STATUS_LIVE

    def test_delayed(self):
        assert ScheduleCrawler._normalize_schedule_status("지연") == GAME_STATUS_DELAYED

    def test_suspended_variants(self):
        assert ScheduleCrawler._normalize_schedule_status("서스펜디드") == GAME_STATUS_SUSPENDED
        assert ScheduleCrawler._normalize_schedule_status("일시정지") == GAME_STATUS_SUSPENDED

    def test_cancelled_variants(self):
        assert ScheduleCrawler._normalize_schedule_status("취소") == GAME_STATUS_CANCELLED
        assert ScheduleCrawler._normalize_schedule_status("우천취소") == GAME_STATUS_CANCELLED
        assert ScheduleCrawler._normalize_schedule_status("경기취소") == GAME_STATUS_CANCELLED

    def test_postponed_variants(self):
        assert ScheduleCrawler._normalize_schedule_status("순연") == GAME_STATUS_POSTPONED
        assert ScheduleCrawler._normalize_schedule_status("연기") == GAME_STATUS_POSTPONED

    def test_unknown_returns_scheduled(self):
        assert ScheduleCrawler._normalize_schedule_status("UNKNOWN") == GAME_STATUS_SCHEDULED

    def test_already_normalized(self):
        assert ScheduleCrawler._normalize_schedule_status("COMPLETED") == GAME_STATUS_COMPLETED
        assert ScheduleCrawler._normalize_schedule_status("LIVE") == GAME_STATUS_LIVE
        assert ScheduleCrawler._normalize_schedule_status("SCHEDULED") == GAME_STATUS_SCHEDULED


class TestExtractGameId:
    def test_basic_url(self):
        result = ScheduleCrawler._extract_game_id("https://example.com?gameId=20250625LGSS0")
        assert result == "20250625LGSS0"

    def test_url_with_extra_params(self):
        result = ScheduleCrawler._extract_game_id("https://example.com?gameId=20250625LGSS0&other=value")
        assert result == "20250625LGSS0"

    def test_url_without_game_id(self):
        result = ScheduleCrawler._extract_game_id("https://example.com?other=value")
        assert result == ""

    def test_empty_string(self):
        result = ScheduleCrawler._extract_game_id("")
        assert result == ""

    def test_game_id_with_ampersand(self):
        result = ScheduleCrawler._extract_game_id("href?gameId=ABC123&section=REVIEW")
        assert result == "ABC123"

    def test_relative_url(self):
        result = ScheduleCrawler._extract_game_id("/Game?gameId=20250625LGSS0")
        assert result == "20250625LGSS0"
