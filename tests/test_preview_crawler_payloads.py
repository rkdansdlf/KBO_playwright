"""
Tests for preview crawler API payload normalization logic.
"""
from src.crawlers.preview_crawler import PreviewCrawler


def test_coerce_api_payload_unwraps_aspnet_d():
    crawler = PreviewCrawler()

    raw = {"d": '[{"G_ID":"20260421HHLG0","AWAY_NM":"한화","HOME_NM":"LG"}]'}
    payload = crawler._coerce_api_payload(raw)
    assert isinstance(payload, list)
    assert payload[0]["G_ID"] == "20260421HHLG0"


def test_extract_list_payload_various_shapes():
    crawler = PreviewCrawler()

    direct_list = [{"G_ID": "20260421HHLG0"}]
    assert crawler._extract_list_payload(direct_list) == direct_list

    wrapped_game = {"game": direct_list}
    assert crawler._extract_list_payload(wrapped_game) == direct_list

    wrapped_result = {"result": {"data": {"data": direct_list}}}
    assert crawler._extract_list_payload(wrapped_result) == []

    wrapped_data = {"data": direct_list}
    assert crawler._extract_list_payload(wrapped_data) == direct_list


def test_parse_lineup_grid_empty_and_ordered_rows():
    crawler = PreviewCrawler()

    assert crawler._parse_lineup_grid([]) == []
    assert crawler._parse_lineup_grid(["{bad-json}"]) == []

    grid = [
        (
            '{"rows":['
            '{"row":[{"Text":"1"},{"Text":"2루수"},{"Text":"박찬호"}]},'
            '{"row":[{"Text":"2"},{"Text":"유격수"},{"Text":"김도영"}]}'
            ']}'
        )
    ]
    parsed = crawler._parse_lineup_grid(grid)
    assert parsed == [
        {"batting_order": 1, "position": "2루수", "player_name": "박찬호"},
        {"batting_order": 2, "position": "유격수", "player_name": "김도영"},
    ]
