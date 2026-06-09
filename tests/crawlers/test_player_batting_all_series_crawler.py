
from src.crawlers.player_batting_all_series_crawler import (
    _extract_player_id_from_href,
    _is_basic2_headers,
    get_series_mapping,
    safe_parse_number,
)


class TestGetSeriesMapping:
    def test_returns_all_series(self):
        mapping = get_series_mapping()
        assert "regular" in mapping
        assert "exhibition" in mapping
        assert "korean_series" in mapping

    def test_regular_has_correct_values(self):
        mapping = get_series_mapping()
        reg = mapping["regular"]
        assert reg["value"] == "0"
        assert reg["league"] == "REGULAR"

    def test_korean_series_values(self):
        mapping = get_series_mapping()
        ks = mapping["korean_series"]
        assert ks["value"] == "7"
        assert ks["league"] == "KOREAN_SERIES"


class TestSafeParseNumber:
    def test_int(self):
        assert safe_parse_number("10", int) == 10

    def test_float(self):
        assert safe_parse_number("0.275", float) == 0.275

    def test_dash_returns_none(self):
        assert safe_parse_number("-", int) is None

    def test_none_returns_none(self):
        assert safe_parse_number(None, int) is None


class TestExtractPlayerIdFromHref:
    def test_extracts_id(self):
        assert _extract_player_id_from_href("playerId=12345") == 12345

    def test_none_returns_none(self):
        assert _extract_player_id_from_href(None) is None

    def test_no_match_returns_none(self):
        assert _extract_player_id_from_href("no-id-here") is None


class TestIsBasic2Headers:
    def test_detects_basic2_headers(self):
        assert _is_basic2_headers(["순위", "선수명", "팀명", "AVG", "BB", "IBB"]) is True

    def test_basic1_headers_return_false(self):
        assert _is_basic2_headers(["순위", "선수명", "팀명", "AVG", "G", "PA", "AB"]) is False

    def test_empty_headers(self):
        assert _is_basic2_headers([]) is False
