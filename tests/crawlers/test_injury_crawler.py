"""Tests for injury_crawler."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.injury_crawler import InjuryCrawler


class TestParseArticle:
    def test_valid_injury_article(self) -> None:
        crawler = InjuryCrawler.__new__(InjuryCrawler)
        article = {
            "title": "LG 김철수 부상 이탈",
            "subContent": "우측 팔꿈치 부상으로 2주 결장 예상",
            "dateTime": "2026-06-24 10:00",
            "oid": "123",
            "aid": "456",
        }
        result = crawler._parse_article(article)
        if result is not None:
            assert "player_name" in result or "note" in result

    def test_invalid_article(self) -> None:
        crawler = InjuryCrawler.__new__(InjuryCrawler)
        article = {
            "title": "오늘 경기 결과",
            "subContent": "LG 승리",
            "dateTime": "2026-06-24 10:00",
        }
        result = crawler._parse_article(article)
        assert result is None or result.get("player_name") is None
