import pytest

from src.crawlers.foreign_player_crawler import ForeignPlayerCrawler


class TestParseArticle:
    def setup_method(self):
        self.crawler = ForeignPlayerCrawler()

    def test_signed_foreign_player(self):
        article = {
            "title": "LG, 새 외국인 투수 영입",
            "subContent": "LG 트윈스가 새로운 외국인 투수 스미스와 계약했습니다.",
            "oid": "123",
            "aid": "456",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["change_type"] == "SIGNED"
        assert result["team_id"] == "LG"

    def test_released_player(self):
        article = {
            "title": "한화, 외국인 타자 방출 결정",
            "subContent": "성적 부진으로 교체",
            "oid": "789",
            "aid": "012",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["change_type"] == "RELEASED"
        assert result["team_id"] == "HH"

    def test_no_keyword_match_returns_none(self):
        article = {
            "title": "오늘의 날씨는 맑음",
            "subContent": "",
        }
        assert self.crawler._parse_article(article) is None

    def test_injury_reason(self):
        article = {
            "title": "KT 외국인 투수 부상으로 교체",
            "subContent": "어깨 부상으로 대체 선수 물색",
            "oid": "111",
            "aid": "222",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["replacement_reason"] == "INJURY"

    def test_performance_reason(self):
        article = {
            "title": "NC, 외국인 타자 성적 부진으로 방출",
            "subContent": "",
            "oid": "333",
            "aid": "444",
        }
        result = self.crawler._parse_article(article)
        assert result["replacement_reason"] == "PERFORMANCE"
