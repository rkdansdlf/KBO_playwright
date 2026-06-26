from __future__ import annotations

from src.crawlers.parking_crawler import ParkingCrawler, PARKING_FEE_PATTERN
from src.crawlers.food_crawler import FoodCrawler, MENU_PATTERN
from src.crawlers.seat_crawler import SeatCrawler, SECTION_PATTERNS
from src.crawlers.ticket_crawler import TicketCrawler


class TestParkingParsePage:
    def test_extracts_fees(self):
        html = "<p>기본요금: 5,000원  추가시간: 2,000원  일일: 10,000원</p>"
        info = {"source_key": "test", "stadium_id": "JAMSIL", "url": "https://example.com"}
        crawler = ParkingCrawler()
        result = crawler._parse_parking_page(html, info)
        assert len(result) == 1
        assert result[0]["lot"]["stadium_id"] == "JAMSIL"
        assert len(result[0]["fee_rules"]) == 3
        assert result[0]["fee_rules"][0]["label"] == "기본"
        assert result[0]["fee_rules"][0]["amount"] == 5000

    def test_no_fees(self):
        html = "<p>No parking info</p>"
        info = {"source_key": "test", "stadium_id": "MUNHAK", "url": "https://example.com"}
        crawler = ParkingCrawler()
        result = crawler._parse_parking_page(html, info)
        assert len(result) == 1
        assert result[0]["fee_rules"] == []

    def test_lot_data_fields(self):
        html = "<p>무료: 0원</p>"
        info = {"source_key": "test", "stadium_id": "DAEGU", "url": "https://example.com"}
        crawler = ParkingCrawler()
        result = crawler._parse_parking_page(html, info)
        lot = result[0]["lot"]
        assert lot["stadium_id"] == "DAEGU"
        assert lot["lot_type"] == "official"
        assert lot["is_event_day_available"] is True
        assert lot["reservation_required"] is False

    def test_parking_fee_pattern(self):
        text = "기본요금: 5,000원"
        match = PARKING_FEE_PATTERN.search(text)
        assert match is not None
        assert match.group(1) == "기본"
        assert match.group(2) == "5,000"


class TestFoodParsePage:
    def test_extracts_menus(self):
        html = "<p>불고기버거: 8,000원  치즈김밥: 3,500원</p>"
        info = {"source_key": "test", "stadium_id": "SAJIK", "url": "https://example.com"}
        crawler = FoodCrawler()
        result = crawler._parse_food_page(html, info)
        assert len(result) == 1
        assert result[0]["vendor"]["stadium_id"] == "SAJIK"
        assert len(result[0]["menus"]) == 2
        assert result[0]["menus"][0]["menu_name"] == "불고기버거"
        assert result[0]["menus"][0]["price"] == 8000

    def test_no_menus(self):
        html = "<p>No food info</p>"
        info = {"source_key": "test", "stadium_id": "CHANGWON", "url": "https://example.com"}
        crawler = FoodCrawler()
        result = crawler._parse_food_page(html, info)
        assert len(result) == 0

    def test_vendor_fields(self):
        html = "<p>핫도그: 4,000원</p>"
        info = {"source_key": "test", "stadium_id": "SAJIK", "url": "https://example.com"}
        crawler = FoodCrawler()
        result = crawler._parse_food_page(html, info)
        vendor = result[0]["vendor"]
        assert vendor["order_method"] == "onsite"
        assert vendor["confidence"] == "low"

    def test_menu_pattern(self):
        text = "불고기버거: 8,000원"
        match = MENU_PATTERN.search(text)
        assert match is not None


class TestSeatParsePage:
    def test_extracts_sections(self):
        html = "<p>블루석 오렌지존 레드석 네이비존</p>"
        info = {"source_key": "test", "stadium_id": "JAMSIL", "url": "https://example.com"}
        crawler = SeatCrawler()
        result = crawler._parse_seat_page(html, "LG", info)
        assert len(result) >= 4
        names = [s["section_name"] for s in result]
        assert "블루석" in names
        assert "오렌지존" in names

    def test_no_sections(self):
        html = "<p>No seat info</p>"
        info = {"source_key": "test", "stadium_id": "JAMSIL", "url": "https://example.com"}
        crawler = SeatCrawler()
        result = crawler._parse_seat_page(html, "LG", info)
        assert len(result) == 0

    def test_deduplication(self):
        html = "<p>블루석 블루석 블루석</p>"
        info = {"source_key": "test", "stadium_id": "JAMSIL", "url": "https://example.com"}
        crawler = SeatCrawler()
        result = crawler._parse_seat_page(html, "LG", info)
        assert len(result) == 1

    def test_section_fields(self):
        html = "<p>골드석</p>"
        info = {"source_key": "test", "stadium_id": "JAMSIL", "url": "https://example.com"}
        crawler = SeatCrawler()
        result = crawler._parse_seat_page(html, "LG", info)
        section = result[0]
        assert section["stadium_id"] == "JAMSIL"
        assert section["section_name"] == "골드석"
        assert section["section_code"] == "골드석"
        assert section["seat_grade"] == "골드석"

    def test_section_patterns(self):
        text = "블루석 오렌지존 1F zone"
        matches = []
        for pattern in SECTION_PATTERNS:
            for m in pattern.finditer(text):
                matches.append(m.group(0))
        assert len(matches) >= 2


class TestTicketAltToTeamCode:
    def test_lg(self):
        assert TicketCrawler._alt_to_team_code("lg_twins") == "LG"

    def test_hh(self):
        assert TicketCrawler._alt_to_team_code("한화이글스") == "HH"

    def test_ss(self):
        assert TicketCrawler._alt_to_team_code("삼성라이온즈") == "SS"

    def test_kt(self):
        assert TicketCrawler._alt_to_team_code("kt_wiz") == "KT"

    def test_ob(self):
        assert TicketCrawler._alt_to_team_code("두산베어스") == "OB"

    def test_lt(self):
        assert TicketCrawler._alt_to_team_code("롯데자이언츠") == "LT"

    def test_ht(self):
        assert TicketCrawler._alt_to_team_code("기아타이거즈") == "HT"

    def test_nc(self):
        assert TicketCrawler._alt_to_team_code("nc_dinos") == "NC"

    def test_sk(self):
        assert TicketCrawler._alt_to_team_code("ssg_landers") == "SK"

    def test_wo(self):
        assert TicketCrawler._alt_to_team_code("키움히어로즈") == "WO"

    def test_unknown(self):
        assert TicketCrawler._alt_to_team_code("unknown_team") is None

    def test_empty(self):
        assert TicketCrawler._alt_to_team_code("") is None


class TestTicketBuildOpenRules:
    def test_returns_list(self):
        rules = TicketCrawler._build_open_rules()
        assert isinstance(rules, list)
        assert len(rules) > 0

    def test_rule_has_required_fields(self):
        rules = TicketCrawler._build_open_rules()
        for rule in rules:
            assert "team_id" in rule
            assert "platform" in rule
            assert "open_offset_days" in rule
            assert "open_time" in rule


class TestTicketTeamCodeToKr:
    def test_lg(self):
        assert TicketCrawler._team_code_to_kr("LG") == "LG"

    def test_hh(self):
        assert TicketCrawler._team_code_to_kr("HH") == "한화"

    def test_ob(self):
        assert TicketCrawler._team_code_to_kr("OB") == "두산"

    def test_ht(self):
        assert TicketCrawler._team_code_to_kr("HT") == "KIA"

    def test_sk(self):
        assert TicketCrawler._team_code_to_kr("SK") == "SSG"

    def test_unknown(self):
        assert TicketCrawler._team_code_to_kr("XX") is None
