"""Tests for foreign_player_crawler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.foreign_player_crawler import ForeignPlayerCrawler


class TestExtractForeignPlayerName:
    def test_extract_korean_name_with_role(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 타자 홍길동과 계약 체결"
        result = crawler._extract_foreign_player_name(text)
        assert result is not None
        assert "홍길동" in result

    def test_extract_english_name_signing(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "LG 외국인 타자 Mike Trout 영입 공식"
        result = crawler._extract_foreign_player_name(text)
        assert result == "Mike Trout"

    def test_extract_korean_name_signing(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "LG 외국인 타자 홍길동 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result == "홍길동"

    def test_extract_name_with_change_type(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 투수 홍길동 교체 발표"
        result = crawler._extract_foreign_player_name(text)
        assert result == "홍길동"

    def test_returns_none_for_no_match(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "오늘 경기 결과 발표"
        result = crawler._extract_foreign_player_name(text)
        assert result is None

    def test_filters_invalid_names(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        text = "외국인 대체 영입"
        result = crawler._extract_foreign_player_name(text)
        assert result is None


class TestDetectChangeType:
    def test_released(self) -> None:
        assert ForeignPlayerCrawler._detect_change_type("외국인 방출") == "RELEASED"
        assert ForeignPlayerCrawler._detect_change_type("웨이버 공시") == "RELEASED"

    def test_replaced(self) -> None:
        assert ForeignPlayerCrawler._detect_change_type("외국인 교체") == "REPLACED"
        assert ForeignPlayerCrawler._detect_change_type("대체 선수") == "REPLACED"

    def test_renewed(self) -> None:
        assert ForeignPlayerCrawler._detect_change_type("재계약 체결") == "RENEWED"

    def test_signed_default(self) -> None:
        assert ForeignPlayerCrawler._detect_change_type("영입 발표") == "SIGNED"


class TestDetectReason:
    def test_injury(self) -> None:
        assert ForeignPlayerCrawler._detect_reason("부상으로 인한 교체") == "INJURY"

    def test_performance(self) -> None:
        assert ForeignPlayerCrawler._detect_reason("성적 부진") == "PERFORMANCE"

    def test_none_for_other(self) -> None:
        assert ForeignPlayerCrawler._detect_reason("새 영입") is None


class TestExtractTeamId:
    def test_lg_team(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("LG 트윈스 외국인 영입")
        assert result == "LG"

    def test_ssg_team(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("SSG 랜더스 외국인 교체")
        assert result == "SSG"

    def test_unknown_team(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        result = crawler._extract_team_id("외국인 영입 발표")
        assert result is None


class TestParseArticle:
    def test_valid_article(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "LG 외국인 투수 John Smith 영입",
            "subContent": "2026년 6월 24일 공식 발표",
            "dateTime": "2026-06-24 10:00",
            "oid": "123",
            "aid": "456",
        }
        result = crawler._parse_article(article)
        assert result is not None
        assert result["player_name"] == "John Smith"
        assert result["team_id"] == "LG"
        assert result["change_type"] == "SIGNED"

    def test_invalid_article_no_name(self) -> None:
        crawler = ForeignPlayerCrawler.__new__(ForeignPlayerCrawler)
        article = {
            "title": "오늘 경기 결과",
            "subContent": "LG 승리",
            "dateTime": "2026-06-24 10:00",
        }
        result = crawler._parse_article(article)
        assert result is None
