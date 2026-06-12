from src.parsers.ticket_parser import (
    PRICE_PATTERN,
    TEAM_CODE_FROM_SOURCE_KEY,
    WEEKEND_PATTERN,
    parse_ticket_page,
)


class TestTicketParser:
    def test_parse_weekday_tickets(self):
        html = "<html><body><p>테이블석 : 150,000원 지정석 : 100,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert len(result) >= 2
        prices = {r["seat_grade"]: r["price"] for r in result}
        assert "테이블석" in prices
        assert "지정석" in prices
        assert prices["테이블석"] == 150000
        assert all(r["day_type"] == "weekday" for r in result)

    def test_parse_weekend_tickets(self):
        html = "<html><body><p>테이블석 : 150,000원</p><p>주말 테이블석 : 180,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        day_types = {(r["seat_grade"], r["day_type"]) for r in result}
        assert ("테이블석", "weekday") in day_types
        assert ("테이블석", "weekend") in day_types

    def test_unknown_source_key_returns_empty(self):
        html = "<html><body><p>테이블석: 150,000원</p></body></html>"
        result = parse_ticket_page(html, "unknown_key", {"season": 2025})
        assert result == []

    def test_empty_html_returns_empty(self):
        result = parse_ticket_page("", "lg_twins_ticket", {"season": 2025})
        assert result == []

    def test_no_price_matches_returns_empty(self):
        html = "<html><body><p>오늘의 경기 일정</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert result == []

    def test_output_schema(self):
        html = "<html><body><p>테이블석 : 150,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        item = result[0]
        assert item["team_id"] == "LG"
        assert item["stadium_id"] == "JAMSIL"
        assert item["season"] == 2025
        assert item["currency"] == "KRW"
        assert item["audience_type"] == "general"

    def test_season_defaults_to_current_year(self):
        from datetime import datetime

        html = "<html><body><p>테이블석 : 150,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket")
        assert result[0]["season"] == datetime.now().year

    def test_all_team_mappings(self):
        expected = {
            "lg_twins_ticket": ("LG", "JAMSIL"),
            "hanwha_eagles_ticket": ("HH", "HANBAT"),
            "samsung_lions_ticket": ("SS", "DAEGU"),
            "kt_wiz_ticket": ("KT", "SUWON"),
            "doosan_bears_ticket": ("OB", "JAMSIL"),
            "lotte_giants_ticket": ("LT", "SAJIK"),
            "kia_tigers_ticket": ("HT", "GWANGJU"),
            "nc_dinos_ticket": ("NC", "CHANGWON"),
            "ssg_landers_ticket": ("SK", "MUNHAK"),
            "kiwoom_heroes_ticket": ("WO", "GOCHEOK"),
        }
        for key, (team, stadium) in expected.items():
            assert TEAM_CODE_FROM_SOURCE_KEY[key] == (team, stadium)

    def test_krw_symbol_variants(self):
        html = "<html><body><p>테이블석 : 150,000￦ 지정석 : 100,000KRW</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        assert len(result) >= 2

    def test_season_from_metadata(self):
        html = "<html><body><p>테이블석 : 150,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": "2024"})
        assert result[0]["season"] == 2024

    def test_zone_suffix_matching(self):
        html = "<html><body><p>프리미엄존 : 200,000원 일반석 : 80,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        grades = [r["seat_grade"] for r in result]
        assert "프리미엄존" in grades
        assert "일반석" in grades

    def test_weekend_without_weekday_does_not_create_weekend(self):
        html = "<html><body><p>주말 테이블석 : 180,000원</p></body></html>"
        result = parse_ticket_page(html, "lg_twins_ticket", {"season": 2025})
        day_types = {(r["seat_grade"], r["day_type"]) for r in result}
        assert ("테이블석", "weekend") not in day_types


class TestPricePatterns:
    def test_price_pattern_basic(self):
        m = PRICE_PATTERN.search("테이블석 : 150,000원")
        assert m
        assert m.group(1) == "테이블석"
        assert m.group(2) == "150,000"

    def test_price_pattern_no_colon(self):
        m = PRICE_PATTERN.search("지정석 100,000원")
        assert m

    def test_weekend_pattern_basic(self):
        m = WEEKEND_PATTERN.search("주말 테이블석 : 180,000원")
        assert m
        assert m.group(1) == "테이블석"
        assert m.group(2) == "180,000"

    def test_weekend_pattern_no_colon(self):
        m = WEEKEND_PATTERN.search("주말 지정석 100,000원")
        assert m
