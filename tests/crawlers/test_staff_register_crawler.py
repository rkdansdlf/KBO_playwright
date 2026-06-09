import pytest
from datetime import date

from src.crawlers.staff_register_crawler import _parse_player_id, _parse_hw, _parse_birth_date, _parse_hands


class TestParsePlayerId:
    def test_extracts_id(self):
        assert _parse_player_id("/Record/Player/HitterDetail/Basic.aspx?playerId=91350") == 91350

    def test_none_returns_none(self):
        assert _parse_player_id(None) is None

    def test_no_match_returns_none(self):
        assert _parse_player_id("/some/page.aspx") is None


class TestParseHw:
    def test_parses_height_weight(self):
        h, w = _parse_hw("185cm, 92kg")
        assert h == 185
        assert w == 92

    def test_no_match_returns_none(self):
        h, w = _parse_hw("No data")
        assert h is None
        assert w is None

    def test_empty_string(self):
        h, w = _parse_hw("")
        assert h is None
        assert w is None


class TestParseBirthDate:
    def test_parses_date(self):
        result = _parse_birth_date("1990-05-15")
        assert result == date(1990, 5, 15)

    def test_no_match_returns_none(self):
        assert _parse_birth_date("Unknown") is None

    def test_empty_returns_none(self):
        assert _parse_birth_date("") is None


class TestParseHands:
    def test_parses_right_throw_right_bat(self):
        throws, bats = _parse_hands("우투우타")
        assert throws == "R"
        assert bats == "R"

    def test_parses_left_throw_left_bat(self):
        throws, bats = _parse_hands("좌투좌타")
        assert throws == "L"
        assert bats == "L"

    def test_no_match_returns_none(self):
        throws, bats = _parse_hands("")
        assert throws is None
        assert bats is None
