import pytest

from src.crawlers.seat_crawler import SeatCrawler


class TestParseSeatPage:
    def setup_method(self):
        self.crawler = SeatCrawler()

    def test_parses_seat_sections(self):
        html = "<html><body>블루석 레드존 골드석</body></html>"
        result = self.crawler._parse_seat_page(html, "LG", {"stadium_id": "JAMSIL"})
        assert len(result) >= 1
        assert all(s["stadium_id"] == "JAMSIL" for s in result)

    def test_deduplicates_sections(self):
        html = "<html><body>블루석 블루석 블루석</body></html>"
        result = self.crawler._parse_seat_page(html, "LG", {"stadium_id": "JAMSIL"})
        blues = [s for s in result if s["section_name"] == "블루석"]
        assert len(blues) == 1

    def test_empty_html_returns_empty_list(self):
        result = self.crawler._parse_seat_page("", "LG", {"stadium_id": "JAMSIL"})
        assert result == []
