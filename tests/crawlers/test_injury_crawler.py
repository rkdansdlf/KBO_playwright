
from src.crawlers.injury_crawler import InjuryCrawler


class TestParseArticle:
    def setup_method(self):
        self.crawler = InjuryCrawler()

    def test_detects_player_and_team(self):
        article = {
            "title": "LG 홍길동선수 어깨 부상",
            "subContent": "어깨 부상으로 이탈",
            "oid": "111",
            "aid": "222",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["player_name"] == "홍길동"
        assert result["team_id"] == "LG"
        assert result["body_part"] == "어깨"

    def test_no_player_match_returns_none(self):
        article = {
            "title": "오늘의 KBO 경기 결과",
            "subContent": "",
        }
        assert self.crawler._parse_article(article) is None

    def test_detects_body_part(self):
        article = {
            "title": "삼성 김철수선수 무릎 부상",
            "subContent": "무릎 부상으로 전력 이탈",
            "oid": "333",
            "aid": "444",
        }
        result = self.crawler._parse_article(article)
        assert result["player_name"] == "김철수"
        assert result["body_part"] == "무릎"
        assert result["team_id"] == "SS"
