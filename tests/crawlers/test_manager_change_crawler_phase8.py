from __future__ import annotations

import pytest

from src.crawlers.manager_change_crawler import ManagerChangeCrawler
from src.utils.naver_helpers import NAVER_TEAM_MAP


class TestManagerNameExtraction:
    def setup_method(self):
        self.crawler = ManagerChangeCrawler()

    def test_standard_manager_format(self):
        result = self.crawler._extract_manager_name("LG 김철수감독 선임")
        assert result == "김철수"

    def test_new_manager_format(self):
        result = self.crawler._extract_manager_name("새 감독 박영수 선임")
        assert result == "박영수"

    def test_interim_format(self):
        result = self.crawler._extract_manager_name("KT 이영호감독대행")
        assert result == "이영호"

    def test_colon_format(self):
        result = self.crawler._extract_manager_name("감독: 김철수")
        assert result == "김철수"

    def test_colon_with_space_format(self):
        result = self.crawler._extract_manager_name("감독 : 이영호")
        assert result == "이영호"

    def test_no_match_returns_none(self):
        result = self.crawler._extract_manager_name("LG 코치 선임")
        assert result is None

    def test_empty_string(self):
        result = self.crawler._extract_manager_name("")
        assert result is None

    def test_excludes_team_name(self):
        result = self.crawler._extract_manager_name("타이거즈 감독 선임")
        assert result is None


class TestDetectReason:
    def test_fired(self):
        assert ManagerChangeCrawler._detect_reason("경질 해고") == "FIRED"

    def test_resign(self):
        assert ManagerChangeCrawler._detect_reason("사임 발표") == "RESIGN"

    def test_resign_sato(self):
        assert ManagerChangeCrawler._detect_reason("사퇴 발표") == "RESIGN"

    def test_interim(self):
        assert ManagerChangeCrawler._detect_reason("대행 선임") == "INTERIM"

    def test_none_for_other(self):
        assert ManagerChangeCrawler._detect_reason("선임 발�") is None

    def test_empty_string(self):
        assert ManagerChangeCrawler._detect_reason("") is None


class TestExtractTeamIdManager:
    def setup_method(self):
        self.crawler = ManagerChangeCrawler()

    def test_all_teams(self):
        for keyword, expected_id in NAVER_TEAM_MAP.items():
            result = self.crawler._extract_team_id(f"{keyword} 감독 선임")
            assert result == expected_id, f"Expected {expected_id} for {keyword}, got {result}"

    def test_unknown_team(self):
        result = self.crawler._extract_team_id("해외 감독 선임")
        assert result is None


class TestParseArticleManager:
    def setup_method(self):
        self.crawler = ManagerChangeCrawler()

    def test_complete_article(self):
        article = {
            "title": "LG 김철수감독 경질",
            "subContent": "성적 부진으로 경질되었습니다.",
            "dateTime": "2026-06-25 14:00",
            "oid": "111",
            "aid": "222",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["new_manager"] == "김철수"
        assert result["team_id"] == "LG"
        assert result["change_reason"] == "FIRED"
        assert result["season"] == 2026

    def test_article_with_unknown_team(self):
        article = {
            "title": "새 감독 박영수 선임",
            "subContent": "",
            "dateTime": "2026-06-25 14:00",
            "oid": "333",
            "aid": "444",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["team_id"] == "UNKNOWN"

    def test_article_no_keyword(self):
        article = {
            "title": "오늘 경기 결과",
            "subContent": "LG 승리",
        }
        result = self.crawler._parse_article(article)
        assert result is None

    def test_article_no_manager_name(self):
        article = {
            "title": "LG 코치 선임 발표",
            "subContent": "",
            "dateTime": "2026-06-25 14:00",
        }
        result = self.crawler._parse_article(article)
        assert result is None

    def test_article_with_excluded_team_name(self):
        article = {
            "title": "타이거즈 감독 선임",
            "subContent": "",
            "dateTime": "2026-06-25 14:00",
        }
        result = self.crawler._parse_article(article)
        assert result is None

    def test_article_preserves_note(self):
        article = {
            "title": "LG 김철수감독 선임 공식",
            "subContent": "",
            "dateTime": "2026-06-25 14:00",
            "oid": "111",
            "aid": "222",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["note"] == "LG 김철수감독 선임 공식"

    def test_article_season_from_date(self):
        article = {
            "title": "LG 김철수감독 선임",
            "subContent": "",
            "dateTime": "2025-01-15 10:00",
            "oid": "111",
            "aid": "222",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["season"] == 2025

    def test_article_url_construction(self):
        article = {
            "title": "LG 김철수감독 선임",
            "subContent": "",
            "dateTime": "2026-06-25 14:00",
            "oid": "123",
            "aid": "456",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert "naver.com" in result["source_url"]

    def test_article_replacement_reason_in_subcontent(self):
        article = {
            "title": "KT 이영호감독 선임",
            "subContent": "경질로 신임 감독 선임",
            "dateTime": "2026-06-25 14:00",
            "oid": "777",
            "aid": "888",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["team_id"] == "KT"
        assert result["new_manager"] == "이영호"

    def test_article_interim(self):
        article = {
            "title": "NC 박철우 감독대행 선임",
            "subContent": "NC 다이노스 감독 사임 후 대행",
            "dateTime": "2026-06-25 14:00",
            "oid": "999",
            "aid": "000",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["change_reason"] == "RESIGN"
