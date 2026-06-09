import pytest

from src.crawlers.daily_roster_crawler import DailyRosterCrawler


class TestCleanCategory:
    def setup_method(self):
        self.crawler = DailyRosterCrawler()

    def test_removes_parenthetical_count(self):
        assert self.crawler._clean_category("투수 (14명)") == "투수"

    def test_no_parenthesis_returns_unchanged(self):
        assert self.crawler._clean_category("포수") == "포수"

    def test_empty_string(self):
        assert self.crawler._clean_category("") == ""
