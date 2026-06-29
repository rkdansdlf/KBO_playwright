"""Unit tests for type_helpers pure functions."""

from __future__ import annotations

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
    def test_int(self) -> None:
        assert to_int(42) == 42

    def test_string(self) -> None:
        assert to_int("123") == 123

    def test_none_default(self) -> None:
        assert to_int(None) == 0

    def test_none_custom_default(self) -> None:
        assert to_int(None, default=5) == 5

    def test_invalid_string(self) -> None:
        assert to_int("abc") == 0


class TestSafeInt:
    def test_valid(self) -> None:
        assert safe_int("42") == 42

    def test_none(self) -> None:
        assert safe_int(None) == 0

    def test_invalid(self) -> None:
        assert safe_int("abc") == 0


class TestSafeIntOrNone:
    def test_valid(self) -> None:
        assert safe_int_or_none("42") == 42

    def test_none(self) -> None:
        assert safe_int_or_none(None) is None

    def test_invalid(self) -> None:
        assert safe_int_or_none("abc") is None


class TestSafeFloat:
    def test_valid(self) -> None:
        assert safe_float("3.14") == pytest.approx(3.14)

    def test_none(self) -> None:
        assert safe_float(None) == 0.0

    def test_invalid(self) -> None:
        assert safe_float("abc") == 0.0


class TestSafeFloatOrNone:
    def test_valid(self) -> None:
        assert safe_float_or_none("3.14") == pytest.approx(3.14)

    def test_none(self) -> None:
        assert safe_float_or_none(None) is None

    def test_invalid(self) -> None:
        assert safe_float_or_none("abc") is None


class TestParseInnings:
    def test_whole_innings(self) -> None:
        assert parse_innings("6") == 6.0

    def test_decimal_format(self) -> None:
        result = parse_innings("6.1")
        assert result > 6.0
        assert result < 7.0

    def test_none(self) -> None:
        assert parse_innings(None) == 0.0

    def test_empty(self) -> None:
        assert parse_innings("") == 0.0


class TestParseInsToOuts:
    def test_whole_innings(self) -> None:
        assert parse_innings_to_outs("6") == 18

    def test_partial_inning(self) -> None:
        result = parse_innings_to_outs("6.1")
        assert result is not None
        assert result > 18

    def test_none(self) -> None:
        assert parse_innings_to_outs(None) is None

    def test_empty(self) -> None:
        assert parse_innings_to_outs("") is None
