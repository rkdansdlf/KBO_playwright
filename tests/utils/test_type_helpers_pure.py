from __future__ import annotations

from src.utils.type_helpers import (
    parse_innings,
    parse_innings_to_outs,
    safe_float_or_none,
    safe_int_or_none,
)


class TestSafeIntOrNone:
    def test_none(self):
        assert safe_int_or_none(None) is None

    def test_valid_int(self):
        assert safe_int_or_none("42") == 42

    def test_zero(self):
        assert safe_int_or_none("0") == 0

    def test_negative(self):
        assert safe_int_or_none("-5") == -5

    def test_invalid_string(self):
        assert safe_int_or_none("abc") is None

    def test_float_string(self):
        assert safe_int_or_none("3.14") is None

    def test_empty_string(self):
        assert safe_int_or_none("") is None

    def test_whitespace(self):
        assert safe_int_or_none("  7  ") == 7

    def test_dash(self):
        assert safe_int_or_none("-") is None


class TestSafeFloatOrNone:
    def test_none(self):
        assert safe_float_or_none(None) is None

    def test_valid_float(self):
        assert safe_float_or_none("3.14") == 3.14

    def test_integer_string(self):
        assert safe_float_or_none("42") == 42.0

    def test_negative(self):
        assert safe_float_or_none("-1.5") == -1.5

    def test_invalid(self):
        assert safe_float_or_none("abc") is None

    def test_empty(self):
        assert safe_float_or_none("") is None


class TestParseInningsToOuts:
    def test_zero(self):
        assert parse_innings_to_outs("0") == 0

    def test_one_inning(self):
        assert parse_innings_to_outs("1") == 3

    def test_two_innings(self):
        assert parse_innings_to_outs("2") == 6

    def test_nine_innings(self):
        assert parse_innings_to_outs("9") == 27

    def test_fraction_one_third(self):
        assert parse_innings_to_outs("1 1/3") == 4

    def test_fraction_two_thirds(self):
        assert parse_innings_to_outs("2 2/3") == 8

    def test_complex(self):
        assert parse_innings_to_outs("5 1/3") == 16

    def test_none(self):
        assert parse_innings_to_outs(None) is None

    def test_empty(self):
        assert parse_innings_to_outs("") is None


class TestParseInnings:
    def test_zero(self):
        assert parse_innings("0") == 0.0

    def test_one(self):
        assert parse_innings("1") == 1.0

    def test_fraction(self):
        assert abs(parse_innings("1/3") - 0.333) < 0.01

    def test_mixed(self):
        assert abs(parse_innings("5 1/3") - 5.333) < 0.01

    def test_none(self):
        assert parse_innings(None) == 0.0

    def test_empty(self):
        assert parse_innings("") == 0.0

    def test_dash(self):
        assert parse_innings("-") == 0.0
