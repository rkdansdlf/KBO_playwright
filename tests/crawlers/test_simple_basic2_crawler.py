
from src.crawlers.simple_basic2_crawler import parse_player_id_from_link, safe_parse_number


class TestSafeParseNumber:
    def test_int_parsing(self):
        assert safe_parse_number("42", int) == 42
        assert safe_parse_number("0", int) == 0

    def test_float_parsing(self):
        assert safe_parse_number("3.14", float) == 3.14

    def test_empty_or_special_returns_none(self):
        assert safe_parse_number("", int) is None
        assert safe_parse_number("-", int) is None
        assert safe_parse_number("N/A", int) is None

    def test_invalid_string_returns_none(self):
        assert safe_parse_number("abc", int) is None


class TestParsePlayerIdFromLink:
    def test_extracts_player_id(self):
        href = "/Record/Player/HitterDetail/Basic.aspx?playerId=67890&year=2025"
        assert parse_player_id_from_link(href) == 67890

    def test_no_player_id_returns_none(self):
        assert parse_player_id_from_link("/page.aspx") is None


