from __future__ import annotations

from src.crawlers.preview_crawler import PreviewCrawler


class TestCoerceApiPayload:
    def test_none_returns_none(self):
        assert PreviewCrawler._coerce_api_payload(None) is None

    def test_empty_string_returns_none(self):
        assert PreviewCrawler._coerce_api_payload("") is None

    def test_whitespace_string_returns_none(self):
        assert PreviewCrawler._coerce_api_payload("   ") is None

    def test_json_string_parsed(self):
        result = PreviewCrawler._coerce_api_payload('{"key": "value"}')
        assert result == {"key": "value"}

    def test_invalid_json_returns_none(self):
        assert PreviewCrawler._coerce_api_payload("{invalid}") is None

    def test_dict_with_d_key(self):
        result = PreviewCrawler._coerce_api_payload({"d": [1, 2, 3]})
        assert result == [1, 2, 3]

    def test_dict_without_d_key(self):
        result = PreviewCrawler._coerce_api_payload({"key": "value"})
        assert result == {"key": "value"}

    def test_list_passthrough(self):
        result = PreviewCrawler._coerce_api_payload([1, 2, 3])
        assert result == [1, 2, 3]

    def test_nested_d_in_list(self):
        result = PreviewCrawler._coerce_api_payload({"d": '{"nested": true}'})
        assert result == {"nested": True}


class TestExtractListPayload:
    def test_list_passthrough(self):
        result = PreviewCrawler._extract_list_payload([1, 2, 3])
        assert result == [1, 2, 3]

    def test_dict_with_game_key(self):
        result = PreviewCrawler._extract_list_payload({"game": [1, 2]})
        assert result == [1, 2]

    def test_dict_with_games_key(self):
        result = PreviewCrawler._extract_list_payload({"games": [3, 4]})
        assert result == [3, 4]

    def test_dict_with_result_key(self):
        result = PreviewCrawler._extract_list_payload({"result": [5, 6]})
        assert result == [5, 6]

    def test_dict_with_data_key(self):
        result = PreviewCrawler._extract_list_payload({"data": [7, 8]})
        assert result == [7, 8]

    def test_empty_dict(self):
        result = PreviewCrawler._extract_list_payload({})
        assert result == []

    def test_none(self):
        result = PreviewCrawler._extract_list_payload(None)
        assert result == []

    def test_nested_list(self):
        result = PreviewCrawler._extract_list_payload({"result": {"game": [1, 2]}})
        assert result == [1, 2]


class TestCleanText:
    def test_none_returns_empty(self):
        assert PreviewCrawler._clean_text(None) == ""

    def test_string_stripped(self):
        assert PreviewCrawler._clean_text("  hello  ") == "hello"

    def test_number_to_string(self):
        assert PreviewCrawler._clean_text(42) == "42"

    def test_empty_string(self):
        assert PreviewCrawler._clean_text("") == ""


class TestToFlag:
    def test_zero_is_false(self):
        assert PreviewCrawler._to_flag(0) is False

    def test_one_is_true(self):
        assert PreviewCrawler._to_flag(1) is True

    def test_string_zero_is_false(self):
        assert PreviewCrawler._to_flag("0") is False

    def test_string_false_is_false(self):
        assert PreviewCrawler._to_flag("false") is False

    def test_string_hello_is_true(self):
        assert PreviewCrawler._to_flag("hello") is True

    def test_none_is_false(self):
        assert PreviewCrawler._to_flag(None) is False


class TestFirstNonEmptyText:
    def test_finds_first_non_empty(self):
        payload = {"a": "", "b": "value", "c": "other"}
        result = PreviewCrawler._first_non_empty_text(payload, ("a", "b", "c"))
        assert result == "value"

    def test_all_empty(self):
        payload = {"a": "", "b": None}
        result = PreviewCrawler._first_non_empty_text(payload, ("a", "b", "c"))
        assert result == ""

    def test_missing_key_skipped(self):
        payload = {"b": "value"}
        result = PreviewCrawler._first_non_empty_text(payload, ("a", "b", "c"))
        assert result == "value"


class TestFirstNonEmptyValue:
    def test_finds_first_non_empty(self):
        payload = {"a": None, "b": 42, "c": 0}
        result = PreviewCrawler._first_non_empty_value(payload, ("a", "b", "c"))
        assert result == 42

    def test_all_none(self):
        payload = {"a": None, "b": None}
        result = PreviewCrawler._first_non_empty_value(payload, ("a", "b", "c"))
        assert result is None


class TestExtractStarterName:
    def test_away_starter(self):
        game = {"AWAY_PIT_P_NM": "김철수"}
        result = PreviewCrawler._extract_starter_name(game, "away")
        assert result == "김철수"

    def test_home_starter(self):
        game = {"HOME_PIT_P_NM": "이영호"}
        result = PreviewCrawler._extract_starter_name(game, "home")
        assert result == "이영호"

    def test_missing_returns_empty(self):
        game = {}
        result = PreviewCrawler._extract_starter_name(game, "away")
        assert result == ""

    def test_fallback_keys(self):
        game = {"AWAY_PITCHER_NM": "박영수"}
        result = PreviewCrawler._extract_starter_name(game, "away")
        assert result == "박영수"


class TestExtractStarterId:
    def test_away_starter_id(self):
        game = {"AWAY_PIT_P_ID": "123"}
        result = PreviewCrawler._extract_starter_id(game, "away")
        assert result == "123"

    def test_home_starter_id(self):
        game = {"HOME_PIT_P_ID": "456"}
        result = PreviewCrawler._extract_starter_id(game, "home")
        assert result == "456"

    def test_missing_returns_none(self):
        game = {}
        result = PreviewCrawler._extract_starter_id(game, "away")
        assert result is None

    def test_fallback_keys(self):
        game = {"AWAY_PITCHER_ID": "789"}
        result = PreviewCrawler._extract_starter_id(game, "away")
        assert result == "789"


class TestExtractLineupAnnounced:
    def test_announced(self):
        lineup_rows = [{"LINEUP_CK": "1"}]
        result = PreviewCrawler._extract_lineup_announced(lineup_rows, fallback=False)
        assert result is True

    def test_not_announced(self):
        lineup_rows = [{"LINEUP_CK": "0"}]
        result = PreviewCrawler._extract_lineup_announced(lineup_rows, fallback=False)
        assert result is False

    def test_fallback(self):
        result = PreviewCrawler._extract_lineup_announced([], fallback=True)
        assert result is True


class TestExtractEmbeddedGameIds:
    def test_dict_with_game_id(self):
        result = PreviewCrawler._extract_embedded_game_ids({"G_ID": "20250625LGSS0"})
        assert "20250625LGSS0" in result

    def test_dict_without_game_id(self):
        result = PreviewCrawler._extract_embedded_game_ids({"other": "value"})
        assert result == set()

    def test_nested_dict_with_game_id(self):
        result = PreviewCrawler._extract_embedded_game_ids({"nested": {"G_ID": "20250625LGSS0"}})
        assert "20250625LGSS0" in result

    def test_list_of_dicts(self):
        result = PreviewCrawler._extract_embedded_game_ids([{"G_ID": "20250625LGSS0"}, {"G_ID": "20250625KTNC0"}])
        assert len(result) == 2

    def test_none(self):
        result = PreviewCrawler._extract_embedded_game_ids(None)
        assert result == set()

    def test_string_ignored(self):
        result = PreviewCrawler._extract_embedded_game_ids("20250625LGSS0")
        assert result == set()


class TestLineupRowsMatchGame:
    def test_matching_game(self):
        lineup_rows = [{"G_ID": "20250625LGSS0"}]
        result = PreviewCrawler._lineup_rows_match_game(lineup_rows, "20250625LGSS0")
        assert result is True

    def test_non_matching_game(self):
        lineup_rows = [{"G_ID": "20250625LGSS0"}]
        result = PreviewCrawler._lineup_rows_match_game(lineup_rows, "20250625KTNC0")
        assert result is False

    def test_empty_rows(self):
        result = PreviewCrawler._lineup_rows_match_game([], "20250625LGSS0")
        assert result is True


class TestParseLineupGrid:
    def test_single_row(self):
        import json

        grid_data = {"rows": [{"row": [{"Text": "1"}, {"Text": "SS"}, {"Text": "김철수"}]}]}
        result = PreviewCrawler._parse_lineup_grid([json.dumps(grid_data)])
        assert len(result) == 1
        assert result[0]["player_name"] == "김철수"

    def test_empty_list(self):
        result = PreviewCrawler._parse_lineup_grid([])
        assert result == []

    def test_multiple_rows(self):
        import json

        grid_data = {
            "rows": [
                {"row": [{"Text": "1"}, {"Text": "SS"}, {"Text": "김철수"}]},
                {"row": [{"Text": "2"}, {"Text": "P"}, {"Text": "이영호"}]},
            ]
        }
        result = PreviewCrawler._parse_lineup_grid([json.dumps(grid_data)])
        assert len(result) == 2
        assert result[1]["player_name"] == "이영호"

    def test_invalid_json(self):
        result = PreviewCrawler._parse_lineup_grid(["invalid json"])
        assert result == []

    def test_non_digit_order_skipped(self):
        import json

        grid_data = {"rows": [{"row": [{"Text": "교체"}, {"Text": "SS"}, {"Text": "김철수"}]}]}
        result = PreviewCrawler._parse_lineup_grid([json.dumps(grid_data)])
        assert result == []
