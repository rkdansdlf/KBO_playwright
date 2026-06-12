"""Tests for src/utils/type_helpers.py."""

import pytest

from src.utils.type_helpers import (
    parse_innings,
    parse_innings_to_outs,
    safe_float,
    safe_float_or_none,
    safe_int,
    safe_int_or_none,
    to_int,
)


class TestToInt:
    def test_none(self):
        assert to_int(None) == 0

    def test_string_number(self):
        assert to_int("42") == 42

    def test_with_commas(self):
        assert to_int("1,234") == 1234

    def test_invalid(self):
        assert to_int("abc") == 0

    def test_custom_default(self):
        assert to_int(None, default=-1) == -1

    def test_float_string_returns_default(self):
        assert to_int("3.14") == 0


class TestSafeInt:
    def test_none(self):
        assert safe_int(None) == 0

    def test_string_number(self):
        assert safe_int("42") == 42

    def test_with_commas(self):
        assert safe_int("1,234") == 1234

    def test_invalid(self):
        assert safe_int("abc") == 0


class TestSafeFloat:
    def test_none(self):
        assert safe_float(None) == 0.0

    def test_string_number(self):
        assert safe_float("3.14") == 3.14

    def test_with_commas(self):
        assert safe_float("1,234.5") == 1234.5

    def test_invalid(self):
        assert safe_float("abc") == 0.0


class TestParseInnings:
    def test_empty(self):
        assert parse_innings(None) == 0.0
        assert parse_innings("") == 0.0

    def test_dash(self):
        assert parse_innings("-") == 0.0

    def test_whole_number(self):
        assert parse_innings("9") == 9.0

    def test_with_commas(self):
        assert parse_innings("1,000") == 1000.0

    def test_fraction_only(self):
        assert parse_innings("1/3") == pytest.approx(0.333, rel=0.01)

    def test_whole_and_fraction(self):
        assert parse_innings("112 1/3") == pytest.approx(112.333, rel=0.01)

    def test_whole_and_fraction_other(self):
        assert parse_innings("9 2/3") == pytest.approx(9.667, rel=0.01)


class TestSafeIntOrNone:
    def test_none(self):
        assert safe_int_or_none(None) is None

    def test_string_number(self):
        assert safe_int_or_none("42") == 42

    def test_with_commas(self):
        assert safe_int_or_none("1,234") == 1234

    def test_empty(self):
        assert safe_int_or_none("") is None

    def test_dash(self):
        assert safe_int_or_none("-") is None

    def test_em_dash(self):
        assert safe_int_or_none("—") is None

    def test_null_string(self):
        assert safe_int_or_none("null") is None

    def test_invalid(self):
        assert safe_int_or_none("abc") is None

    def test_float_string_returns_none(self):
        assert safe_int_or_none("3.14") is None


class TestSafeFloatOrNone:
    def test_none(self):
        assert safe_float_or_none(None) is None

    def test_string_number(self):
        assert safe_float_or_none("3.14") == 3.14

    def test_with_commas(self):
        assert safe_float_or_none("1,234.5") == 1234.5

    def test_empty(self):
        assert safe_float_or_none("") is None

    def test_dash(self):
        assert safe_float_or_none("-") is None

    def test_invalid(self):
        assert safe_float_or_none("abc") is None


class TestParseInningsToOuts:
    def test_none(self):
        assert parse_innings_to_outs(None) is None

    def test_empty(self):
        assert parse_innings_to_outs("") is None

    def test_dash(self):
        assert parse_innings_to_outs("-") is None

    def test_em_dash(self):
        assert parse_innings_to_outs("—") is None

    def test_whole_number(self):
        assert parse_innings_to_outs("5") == 15

    def test_with_fraction(self):
        assert parse_innings_to_outs("5 1/3") == 16

    def test_fraction_only(self):
        assert parse_innings_to_outs("2/3") == 2

    def test_unicode_fraction(self):
        assert parse_innings_to_outs("4 ⅔") == 14

    def test_decimal_notation(self):
        assert parse_innings_to_outs("5.1") == 16

    def test_colon_notation(self):
        assert parse_innings_to_outs("5:1") == 16

    def test_zero(self):
        assert parse_innings_to_outs("0") == 0

    def test_decimal_fraction_only(self):
        assert parse_innings_to_outs("0.2") == 2
