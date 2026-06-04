import pytest

from src.parsers.ticket_parser import parse_ticket_page, PRICE_PATTERN, WEEKEND_PATTERN


class TestTicketParser:
    def test_price_pattern_matches_korean_seats(self):
        text = "테이블석 : 150,000원 지정석: 120,000원"
        matches = PRICE_PATTERN.findall(text)
        assert len(matches) == 2
        assert matches[0] == ("테이블석", "150,000")
        assert matches[1] == ("지정석", "120,000")

    def test_price_pattern_zone_variants(self):
        text = "오렌지존: 50,000원 블루zone 45,000원 레드Zone 40,000원"
        matches = PRICE_PATTERN.findall(text)
        assert len(matches) == 3

    def test_weekend_pattern(self):
        text = "주말 테이블석 : 180,000원 주말 지정석: 150,000원"
        matches = WEEKEND_PATTERN.findall(text)
        assert len(matches) == 2
        assert matches[0] == ("테이블석", "180,000")

    def test_parse_inline_lg_page(self):
        html = """
        <html><body>
        <h1>LG 트윈스 티켓 안내</h1>
        <p>테이블석 : 150,000원</p>
        <p>지정석: 120,000원</p>
        <p>주말 테이블석 : 180,000원</p>
        <p>주말 지정석: 150,000원</p>
        <p>파크존: 70,000원</p>
        </body></html>
        """
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert len(result) >= 5
        lg_items = [r for r in result if r["team_id"] == "LG"]
        assert len(lg_items) >= 5
        weekday = [r for r in lg_items if r["day_type"] == "weekday"]
        weekend = [r for r in lg_items if r["day_type"] == "weekend"]
        assert len(weekday) >= 3
        assert len(weekend) >= 2

    def test_season_from_metadata(self):
        html = "<html><body><p>테이블석 : 150,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2024})
        assert result[0]["season"] == 2024

    def test_default_season_is_current_year(self):
        from datetime import datetime
        html = "<html><body><p>테이블석 : 150,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket")
        assert result[0]["season"] == datetime.now().year

    def test_unknown_source_key_returns_empty(self):
        result = parse_ticket_page("<html></html>", "unknown_source")
        assert result == []

    def test_output_schema(self):
        html = "<html><body><p>테이블석 : 150,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert len(result) == 1
        item = result[0]
        assert item["team_id"] == "LG"
        assert item["stadium_id"] == "JAMSIL"
        assert item["season"] == 2025
        assert item["seat_grade"] == "테이블석"
        assert item["day_type"] == "weekday"
        assert item["price"] == 150000
        assert item["currency"] == "KRW"
        assert item["audience_type"] == "general"
