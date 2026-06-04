import pytest

from src.parsers.food_parser import parse_food, MENU_PATTERN, STADIUM_FROM_SOURCE_KEY


class TestFoodParser:
    def test_menu_pattern_matches(self):
        text = "떡볶이: 3,000원 김밥: 2,500원 핫도그 3,000원"
        matches = MENU_PATTERN.findall(text)
        assert len(matches) == 3

    def test_parse_lotte_food(self):
        html = """
        <html><body>
        <h1>사직구장 먹거리</h1>
        <p>떡볶이: 3,000원</p>
        <p>김밥: 2,500원</p>
        <p>핫도그: 3,000원</p>
        <p>음료수: 1,000원</p>
        </body></html>
        """
        result = parse_food(html, "lotte_giants_fnb")
        assert len(result) == 1
        vendor = result[0]
        assert vendor["vendor"]["stadium_id"] == "SAJIK"
        assert "SAJIK" in vendor["vendor"]["vendor_name"]
        assert len(vendor["menus"]) == 4

    def test_unknown_source_key_defaults(self):
        result = parse_food("<html><body><p>떡볶이: 3,000원</p></body></html>", "unknown")
        assert len(result) == 1
        assert result[0]["vendor"]["stadium_id"] == "UNKNOWN"

    def test_no_menus_returns_empty(self):
        result = parse_food("<html><body><p>식당 안내</p></body></html>", "lotte_giants_fnb")
        assert result == []

    def test_output_schema(self):
        html = "<html><body><p>떡볶이: 3,000원</p></body></html>"
        result = parse_food(html, "lotte_giants_fnb")
        assert len(result) == 1
        entry = result[0]
        assert "vendor" in entry
        assert "menus" in entry
        vendor = entry["vendor"]
        assert vendor["stadium_id"] == "SAJIK"
        assert vendor["order_method"] == "onsite"
        assert vendor["confidence"] == "low"
        assert len(entry["menus"]) == 1
        assert entry["menus"][0]["menu_name"] == "떡볶이"
        assert entry["menus"][0]["price"] == 3000
        assert entry["menus"][0]["category"] == "etc"


class TestFoodConstants:
    def test_stadium_mappings(self):
        assert STADIUM_FROM_SOURCE_KEY["lotte_giants_fnb"] == "SAJIK"
        assert STADIUM_FROM_SOURCE_KEY["nc_dinos_food_seat"] == "CHANGWON"
        assert STADIUM_FROM_SOURCE_KEY["gujangfood_com"] == "UNKNOWN"
