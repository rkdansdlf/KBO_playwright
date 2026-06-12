from src.crawlers.game_detail_crawler import (
    HITTER_FLOAT_KEYS,
    HITTER_HEADER_MAP,
    PITCHER_FLOAT_KEYS,
    PITCHER_HEADER_MAP,
    GameDetailCrawler,
)
from src.utils.type_helpers import safe_int_or_none


class TestHitterHeaderMap:
    def test_contains_expected_keys(self):
        assert HITTER_HEADER_MAP["타석"] == "plate_appearances"
        assert HITTER_HEADER_MAP["타율"] == "avg"
        assert HITTER_HEADER_MAP["OPS"] == "ops"

    def test_comprehensive_coverage(self):
        assert len(HITTER_HEADER_MAP) >= 20


class TestPitcherHeaderMap:
    def test_contains_expected_keys(self):
        assert PITCHER_HEADER_MAP["ERA"] == "era"
        assert PITCHER_HEADER_MAP["WHIP"] == "whip"
        assert PITCHER_HEADER_MAP["K/BB"] == "kbb"

    def test_comprehensive_coverage(self):
        assert len(PITCHER_HEADER_MAP) >= 20


class TestFloatKeys:
    def test_hitter_float_keys(self):
        assert "avg" in HITTER_FLOAT_KEYS
        assert "obp" in HITTER_FLOAT_KEYS
        assert "slg" in HITTER_FLOAT_KEYS

    def test_pitcher_float_keys(self):
        assert "era" in PITCHER_FLOAT_KEYS
        assert "whip" in PITCHER_FLOAT_KEYS
        assert "k_per_nine" in PITCHER_FLOAT_KEYS


class TestSafeInt:
    def test_normal_int(self):
        assert safe_int_or_none("42") == 42

    def test_none_or_empty(self):
        assert safe_int_or_none(None) is None
        assert safe_int_or_none("") is None
        assert safe_int_or_none("-") is None

    def test_with_commas(self):
        assert safe_int_or_none("1,234") == 1234


class TestParseDurationMinutes:
    def setup_method(self):
        self.crawler = GameDetailCrawler()

    def test_normal_duration(self):
        assert self.crawler._parse_duration_minutes("2:35") == 155

    def test_one_hour(self):
        assert self.crawler._parse_duration_minutes("1:00") == 60

    def test_none_or_invalid(self):
        assert self.crawler._parse_duration_minutes(None) is None
        assert self.crawler._parse_duration_minutes("abc") is None


class TestParseSeasonYear:
    def setup_method(self):
        self.crawler = GameDetailCrawler()

    def test_extracts_year(self):
        assert self.crawler._parse_season_year("20250412SKLG0") == 2025

    def test_none_returns_none(self):
        assert self.crawler._parse_season_year(None) is None

    def test_short_string(self):
        assert self.crawler._parse_season_year("123") is None
