"""
Integration tests for food_parser with realistic fixture HTML.
"""

from __future__ import annotations

from src.parsers.food_parser import parse_food


def test_food_parses_menus():
    html = """<html><body>
        <h2>사직구장 먹거리</h2>
        <ul class="menu">
            <li>떡볶이: 3,000원</li>
            <li>김밥: 2,500원</li>
            <li>핫도그: 3,000원</li>
            <li>콜라: 1,500원</li>
            <li>맥주: 5,000원</li>
        </ul>
    </body></html>"""
    result = parse_food(html, "lotte_giants_fnb")
    assert len(result) == 1
    menus = result[0]["menus"]
    assert len(menus) == 5
    names = [m["menu_name"] for m in menus]
    assert any("떡볶이" in n for n in names)


def test_food_schema():
    html = "<html><body>떡볶이: 3,000원</body></html>"
    result = parse_food(html, "lotte_giants_fnb")
    assert result[0]["vendor"]["stadium_id"] == "SAJIK"
    assert result[0]["vendor"]["order_method"] == "onsite"
