from __future__ import annotations

import pytest

from src.utils.player_positions import (
    PositionCode,
    RAW_MAP,
    get_primary_position,
    is_battery,
    is_infield,
    is_outfield,
    normalize_position,
)


class TestNormalizePosition:
    def test_none_returns_empty(self):
        assert normalize_position(None) == []

    def test_empty_returns_empty(self):
        assert normalize_position("") == []

    def test_dash_returns_empty(self):
        assert normalize_position("-") == []

    def test_whitespace_returns_empty(self):
        assert normalize_position("   ") == []

    def test_pitcher(self):
        assert normalize_position("투") == [PositionCode.P]

    def test_catcher(self):
        assert normalize_position("포") == [PositionCode.C]

    def test_shortstop(self):
        assert normalize_position("유") == [PositionCode.SS]

    def test_center_field(self):
        assert normalize_position("중") == [PositionCode.CF]

    def test_composite_position(self):
        result = normalize_position("타一")
        assert PositionCode.PH in result
        assert PositionCode.B1 in result

    def test_composite_ss_3b(self):
        result = normalize_position("유三")
        assert PositionCode.SS in result
        assert PositionCode.B3 in result

    def test_numeric_digit(self):
        assert normalize_position("7") == [PositionCode.LF]

    def test_unknown_character(self):
        result = normalize_position("X")
        assert result == [PositionCode.UNKNOWN]

    def test_stripped_input(self):
        assert normalize_position("  투  ") == [PositionCode.P]


class TestGetPrimaryPosition:
    def test_none_returns_unknown(self):
        assert get_primary_position(None) == PositionCode.UNKNOWN

    def test_empty_returns_unknown(self):
        assert get_primary_position("") == PositionCode.UNKNOWN

    def test_single_position(self):
        assert get_primary_position("투") == PositionCode.P

    def test_composite_returns_last(self):
        assert get_primary_position("타一") == PositionCode.B1

    def test_pr_position(self):
        assert get_primary_position("주") == PositionCode.PR


class TestPositionChecks:
    def test_infield_positions(self):
        assert is_infield(PositionCode.B1) is True
        assert is_infield(PositionCode.SS) is True
        assert is_infield(PositionCode.P) is False

    def test_outfield_positions(self):
        assert is_outfield(PositionCode.LF) is True
        assert is_outfield(PositionCode.CF) is True
        assert is_outfield(PositionCode.SS) is False

    def test_battery_positions(self):
        assert is_battery(PositionCode.P) is True
        assert is_battery(PositionCode.C) is True
        assert is_battery(PositionCode.SS) is False


class TestRawMap:
    def test_all_korean_characters_mapped(self):
        korean_chars = "투포二三좌중우지타주"
        for char in korean_chars:
            assert char in RAW_MAP, f"Character '{char}' not in RAW_MAP"

    def test_numeric_digits_mapped(self):
        for digit in "123456789":
            assert digit in RAW_MAP
