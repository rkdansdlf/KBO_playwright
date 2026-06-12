from src.parsers.food_parser import (
    MENU_PATTERN,
    STADIUM_FROM_SOURCE_KEY,
    parse_food,
)


class TestFoodParser:
    def test_parse_basic_food(self):
        html = "<html><body><p>떡볶이: 3,000원 김밥: 2,500원</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb")
        assert len(result) == 1
        vendor = result[0]
        assert vendor["vendor"]["stadium_id"] == "SAJIK"
        assert vendor["vendor"]["vendor_name"] == "SAJIK 구장 매점"
        assert vendor["vendor"]["order_method"] == "onsite"
        assert vendor["vendor"]["confidence"] == "low"
        assert len(vendor["menus"]) == 2
        assert vendor["menus"][0]["menu_name"] == "떡볶이"
        assert vendor["menus"][0]["price"] == 3000
        assert vendor["menus"][0]["category"] == "etc"

    def test_empty_html_returns_empty(self):
        result = parse_food("", "lotte_giants_fnb")
        assert result == []

    def test_no_menu_matches_returns_empty(self):
        html = "<html><body><p>매점 운영 안내</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb")
        assert result == []

    def test_unknown_source_key(self):
        html = "<html><body><p>콜라: 1,000원</p></body></html>"
        result = parse_food(html, "unknown_key")
        assert len(result) == 1
        assert result[0]["vendor"]["stadium_id"] == "UNKNOWN"

    def test_nc_dinos_food_seat(self):
        html = "<html><body><p>치킨: 15,000원</p></body></html>"
        result = parse_food(html, "nc_dinos_food_seat")
        assert result[0]["vendor"]["stadium_id"] == "CHANGWON"

    def test_gujangfood_com_source(self):
        html = "<html><body><p>핫도그: 2,000원</p></body></html>"
        result = parse_food(html, "gujangfood_com")
        assert result[0]["vendor"]["stadium_id"] == "UNKNOWN"

    def test_multiple_menus_extracted(self):
        html = "<html><body><p>콜라 1,000원 환타 1,000원 커피 1,500원</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb")
        assert len(result[0]["menus"]) == 3

    def test_menu_with_colon(self):
        html = "<html><body><p>콜라:1,000원 환타:1,000원</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb")
        assert len(result[0]["menus"]) == 2

    def test_large_price(self):
        html = "<html><body><p>프리미엄세트: 150,000원</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb")
        assert result[0]["menus"][0]["price"] == 150000

    def test_english_menu_names(self):
        html = "<html><body><p>Hot Dog: 3,000원 Coke: 1,500원</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb")
        assert len(result[0]["menus"]) == 2

    def test_with_metadata(self):
        html = "<html><body><p>김밥: 2,000원</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb", {"season": 2025})
        assert len(result) == 1


class TestFoodMenuPattern:
    def test_basic_pattern(self):
        m = MENU_PATTERN.search("떡볶이: 3,000원")
        assert m
        assert m.group(1).strip() == "떡볶이"
        assert m.group(2) == "3,000"

    def test_pattern_without_colon(self):
        m = MENU_PATTERN.search("김밥 2,000원")
        assert m

    def test_pattern_no_match_for_non_price(self):
        assert not MENU_PATTERN.search("메뉴 안내")
        assert not MENU_PATTERN.search("운영시간")

    def test_stadium_mappings(self):
        assert STADIUM_FROM_SOURCE_KEY["lotte_giants_fnb"] == "SAJIK"
        assert STADIUM_FROM_SOURCE_KEY["nc_dinos_food_seat"] == "CHANGWON"
        assert STADIUM_FROM_SOURCE_KEY["gujangfood_com"] == "UNKNOWN"
