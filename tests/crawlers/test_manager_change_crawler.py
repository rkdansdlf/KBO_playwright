
from src.crawlers.manager_change_crawler import ManagerChangeCrawler


class TestParseArticle:
    def setup_method(self):
        self.crawler = ManagerChangeCrawler()

    def test_new_manager_signed(self):
        article = {
            "title": "LG 김철수감독 선임",
            "subContent": "LG 트윈스가 김철수감독을 선임했습니다.",
            "oid": "111",
            "aid": "222",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["new_manager"] == "김철수"
        assert result["team_id"] == "LG"
        assert result["change_reason"] is None

    def test_manager_fired(self):
        article = {
            "title": "한화 박영수감독 경질",
            "subContent": "성적 부진으로 경질되었습니다.",
            "oid": "333",
            "aid": "444",
        }
        result = self.crawler._parse_article(article)
        assert result is not None
        assert result["new_manager"] == "박영수"
        assert result["change_reason"] == "FIRED"

    def test_no_keyword_returns_none(self):
        article = {
            "title": "오늘의 야구 소식",
            "subContent": "",
        }
        assert self.crawler._parse_article(article) is None

    def test_interim_manager(self):
        article = {
            "title": "KT 이영호감독대행",
            "subContent": "KT 위즈가 이영호코치를 감독대행으로 선임했습니다.",
            "oid": "555",
            "aid": "666",
        }
        result = self.crawler._parse_article(article)
        assert result["new_manager"] == "이영호"
        assert result["change_reason"] == "INTERIM"
