import pytest

from src.crawlers.player_pitching_all_series_crawler import (
    _map_pitcher_basic1_stats,
    PitcherStats,
    extract_player_id,
    normalize_header,
)
from src.utils.type_helpers import (
    parse_innings,
    parse_innings_to_outs,
    safe_float_or_none,
    safe_int_or_none,
)


class TestNormalizeHeader:
    def test_simple_text(self):
        assert normalize_header("ERA") == "ERA"

    def test_strips_non_breaking_space(self):
        assert normalize_header("WHIP\xa0") == "WHIP"

    def test_takes_first_part(self):
        assert normalize_header("G        (경기)") == "G"

    def test_none_returns_empty(self):
        assert normalize_header(None) == ""


class TestSafeInt:
    def test_normal_int(self):
        assert safe_int_or_none("42") == 42

    def test_dash_returns_none(self):
        assert safe_int_or_none("-") is None

    def test_en_dash_returns_none(self):
        assert safe_int_or_none("–") is None

    def test_none_returns_none(self):
        assert safe_int_or_none(None) is None

    def test_float_string_returns_none(self):
        assert safe_int_or_none("3.14") is None


class TestSafeFloat:
    def test_normal_float(self):
        assert safe_float_or_none("3.14") == 3.14

    def test_dash_returns_none(self):
        assert safe_float_or_none("-") is None

    def test_none_returns_none(self):
        assert safe_float_or_none(None) is None


class TestParseInnings:
    def test_whole_innings(self):
        assert parse_innings("9") == 9.0
        assert parse_innings_to_outs("9") == 27

    def test_innings_with_fraction(self):
        assert parse_innings("6 2/3") == pytest.approx(6.67, rel=0.01)
        assert parse_innings_to_outs("6 2/3") == 20

    def test_innings_with_decimal(self):
        assert parse_innings("7.1") == 7.1
        assert parse_innings_to_outs("7.1") == 22

    def test_empty_returns_default(self):
        assert parse_innings("") == 0.0
        assert parse_innings(None) == 0.0
        assert parse_innings_to_outs("") is None
        assert parse_innings_to_outs(None) is None

    def test_dash_returns_default(self):
        assert parse_innings("-") == 0.0
        assert parse_innings_to_outs("-") is None


class TestExtractPlayerId:
    def test_extracts_id(self):
        assert extract_player_id("playerId=54321") == 54321

    def test_none_returns_none(self):
        assert extract_player_id(None) is None

    def test_no_match_returns_none(self):
        assert extract_player_id("no-match") is None


def test_by_team_preserves_one_pitching_row_per_player_team():
    pitchers = {}
    for team_name in ("LG", "DB"):
        _map_pitcher_basic1_stats(
            {
                "player_id": 7,
                "player_name": "Test",
                "team_name": team_name,
                "raw": {"G": "1", "IP": "1", "ERA": "0.00"},
            },
            2025,
            "REGULAR",
            pitchers,
            None,
            preserve_team_splits=True,
        )

    assert set(pitchers) == {(7, "LG"), (7, "DB")}
    assert all(isinstance(stats, PitcherStats) for stats in pitchers.values())
