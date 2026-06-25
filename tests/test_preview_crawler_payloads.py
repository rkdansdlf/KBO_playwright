"""Tests for preview crawler API payload normalization logic.
"""

import asyncio

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
    assert crawler._extract_list_payload(wrapped_result) == direct_list

    wrapped_data = {"data": direct_list}
    assert crawler._extract_list_payload(wrapped_data) == direct_list


def test_extract_starter_fields_from_game_list_variants():
    crawler = PreviewCrawler()

    current_shape = {
        "T_PIT_P_NM": "문동주 ",
        "B_PIT_P_NM": "곽빈 ",
        "T_PIT_P_ID": 52701,
        "B_PIT_P_ID": 64213,
    }
    assert crawler._extract_starter_name(current_shape, "away") == "문동주"
    assert crawler._extract_starter_name(current_shape, "home") == "곽빈"
    assert crawler._extract_starter_id(current_shape, "away") == 52701
    assert crawler._extract_starter_id(current_shape, "home") == 64213

    alias_shape = {
        "AWAY_PIT_P_NM": "하트",
        "HOME_PITCHER_NM": "원태인",
        "AWAY_PIT_P_ID": "54780",
        "HOME_PITCHER_ID": "69446",
    }
    assert crawler._extract_starter_name(alias_shape, "away") == "하트"
    assert crawler._extract_starter_name(alias_shape, "home") == "원태인"
    assert crawler._extract_starter_id(alias_shape, "away") == "54780"
    assert crawler._extract_starter_id(alias_shape, "home") == "69446"


def test_extract_lineup_announced_from_lineup_analysis_header():
    crawler = PreviewCrawler()

    assert crawler._extract_lineup_announced([[{"LINEUP_CK": False}]], fallback=True) is False
    assert crawler._extract_lineup_announced([[{"LINEUP_CK": "1"}]], fallback=False) is True
    assert crawler._extract_lineup_announced([], fallback=True) is True


def test_crawl_preview_recovers_starters_and_lineup_flag_from_current_payloads():
    grid = [
        (
            '{"rows":['
            '{"row":[{"Text":"1"},{"Text":"중견수"},{"Text":"이원석"}]},'
            '{"row":[{"Text":"2"},{"Text":"우익수"},{"Text":"페라자"}]}'
            "]}"
        )
    ]

    class FakePreviewCrawler(PreviewCrawler):
        async def _fetch_api_json(self, url, form, referer, page=None):
            if url == self.GAME_LIST_URL:
                return {
                    "game": [
                        {
                            "G_ID": "20260602HHOB0",
                            "SEASON_ID": 2026,
                            "LE_ID": 1,
                            "SR_ID": 0,
                            "AWAY_NM": "한화",
                            "HOME_NM": "두산",
                            "T_PIT_P_NM": "문동주 ",
                            "B_PIT_P_NM": "곽빈 ",
                            "T_PIT_P_ID": 52701,
                            "B_PIT_P_ID": 64213,
                            "START_PIT_CK": 0,
                            "LINEUP_CK": 0,
                        }
                    ]
                }
            if url == self.LINEUP_URL:
                return [[{"LINEUP_CK": "1"}], [], [], grid, grid]
            return None

    previews = asyncio.run(FakePreviewCrawler(request_delay=0).crawl_preview_for_date("20260602"))

    assert len(previews) == 1
    preview = previews[0]
    assert preview["away_starter"] == "문동주"
    assert preview["home_starter"] == "곽빈"
    assert preview["away_starter_id"] == 52701
    assert preview["home_starter_id"] == 64213
    assert preview["start_pitcher_announced"] is True
    assert preview["lineup_announced"] is True
    assert len(preview["away_lineup"]) == 2
    assert len(preview["home_lineup"]) == 2


def test_crawl_preview_ignores_stale_lineup_payload_but_keeps_game_list_starters():
    stale_grid = [
        (
            '{"rows":['
            '{"row":[{"Text":"1"},{"Text":"유격수"},{"Text":"김주원"}]},'
            '{"row":[{"Text":"2"},{"Text":"좌익수"},{"Text":"이우성"}]},'
            '{"row":[{"Text":"3"},{"Text":"1루수"},{"Text":"박민우"}]},'
            '{"row":[{"Text":"4"},{"Text":"지명타자"},{"Text":"박건우"}]},'
            '{"row":[{"Text":"5"},{"Text":"우익수"},{"Text":"권희동"}]},'
            '{"row":[{"Text":"6"},{"Text":"3루수"},{"Text":"서호철"}]},'
            '{"row":[{"Text":"7"},{"Text":"중견수"},{"Text":"천재환"}]},'
            '{"row":[{"Text":"8"},{"Text":"포수"},{"Text":"김형준"}]},'
            '{"row":[{"Text":"9"},{"Text":"2루수"},{"Text":"김한별"}]}'
            "]}"
        )
    ]

    class FakePreviewCrawler(PreviewCrawler):
        async def _fetch_api_json(self, url, form, referer, page=None):
            if url == self.GAME_LIST_URL:
                return {
                    "game": [
                        {
                            "G_ID": "20260614NCKT0",
                            "SEASON_ID": 2026,
                            "LE_ID": 1,
                            "SR_ID": 0,
                            "AWAY_NM": "NC",
                            "HOME_NM": "KT",
                            "T_PIT_P_NM": "김준원 ",
                            "B_PIT_P_NM": "고영표 ",
                            "T_PIT_P_ID": 54910,
                            "B_PIT_P_ID": 64001,
                            "START_PIT_CK": 1,
                            "LINEUP_CK": 0,
                        }
                    ]
                }
            if url == self.LINEUP_URL:
                return [
                    [{"LINEUP_CK": False}],
                    [{"T_ID": "KT", "G_ID": "20260613NCKT0"}],
                    [{"T_ID": "NC", "G_ID": "20260613NCKT0"}],
                    stale_grid,
                    stale_grid,
                ]
            return None

    previews = asyncio.run(FakePreviewCrawler(request_delay=0).crawl_preview_for_date("20260614"))

    assert len(previews) == 1
    preview = previews[0]
    assert preview["game_id"] == "20260614NCKT0"
    assert preview["away_starter"] == "김준원"
    assert preview["away_starter_id"] == 54910
    assert preview["home_starter"] == "고영표"
    assert preview["home_starter_id"] == 64001
    assert preview["start_pitcher_announced"] is True
    assert preview["lineup_announced"] is False
    assert preview["away_lineup"] == []
    assert preview["home_lineup"] == []


def test_parse_lineup_grid_empty_and_ordered_rows():
    crawler = PreviewCrawler()

    assert crawler._parse_lineup_grid([]) == []
    assert crawler._parse_lineup_grid(["{bad-json}"]) == []

    grid = [
        (
            '{"rows":['
            '{"row":[{"Text":"1"},{"Text":"2루수"},{"Text":"박찬호"}]},'
            '{"row":[{"Text":"2"},{"Text":"유격수"},{"Text":"김도영"}]}'
            "]}"
        )
    ]
    parsed = crawler._parse_lineup_grid(grid)
    assert parsed == [
        {"batting_order": 1, "position": "2루수", "player_name": "박찬호"},
        {"batting_order": 2, "position": "유격수", "player_name": "김도영"},
    ]
