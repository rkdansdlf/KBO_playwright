from __future__ import annotations

from src.utils.player_positions import (
    PositionCode,
    get_primary_position,
    is_battery,
    is_infield,
    is_outfield,
    normalize_position,
)


class TestPositionCode:
    def test_pitcher(self):
        assert PositionCode.P == "P"

    def test_catcher(self):
        assert PositionCode.C == "C"

    def test_first_base(self):
        assert PositionCode.B1 == "1B"

    def test_shortstop(self):
        assert PositionCode.SS == "SS"

    def test_unknown(self):
        assert PositionCode.UNKNOWN == "UNKNOWN"


class TestNormalizePosition:
    def test_empty_string(self):
        assert normalize_position("") == []

    def test_none(self):
        assert normalize_position(None) == []

    def test_dash(self):
        assert normalize_position("-") == []

    def test_pitcher(self):
        result = normalize_position("투")
        assert PositionCode.P in result

    def test_catcher(self):
        result = normalize_position("포")
        assert PositionCode.C in result

    def test_first_base(self):
        result = normalize_position("一")
        assert PositionCode.B1 in result

    def test_composite(self):
        result = normalize_position("타一")
        assert PositionCode.PH in result
        assert PositionCode.B1 in result

    def test_unknown_char(self):
        result = normalize_position("X")
        assert result == [PositionCode.UNKNOWN]


class TestGetPrimaryPosition:
    def test_pitcher(self):
        assert get_primary_position("투") == PositionCode.P

    def test_ph_then_1b(self):
        assert get_primary_position("타一") == PositionCode.B1

    def test_empty(self):
        assert get_primary_position("") == PositionCode.UNKNOWN

    def test_none(self):
        assert get_primary_position(None) == PositionCode.UNKNOWN


class TestIsInfield:
    def test_first_base(self):
        assert is_infield(PositionCode.B1) is True

    def test_second_base(self):
        assert is_infield(PositionCode.B2) is True

    def test_third_base(self):
        assert is_infield(PositionCode.B3) is True

    def test_shortstop(self):
        assert is_infield(PositionCode.SS) is True

    def test_pitcher(self):
        assert is_infield(PositionCode.P) is False

    def test_outfield(self):
        assert is_infield(PositionCode.LF) is False


class TestIsOutfield:
    def test_lf(self):
        assert is_outfield(PositionCode.LF) is True

    def test_cf(self):
        assert is_outfield(PositionCode.CF) is True

    def test_rf(self):
        assert is_outfield(PositionCode.RF) is True

    def test_pitcher(self):
        assert is_outfield(PositionCode.P) is False

    def test_infield(self):
        assert is_outfield(PositionCode.B1) is False


class TestIsBattery:
    def test_pitcher(self):
        assert is_battery(PositionCode.P) is True

    def test_catcher(self):
        assert is_battery(PositionCode.C) is True

    def test_infield(self):
        assert is_battery(PositionCode.B1) is False

    def test_outfield(self):
        assert is_battery(PositionCode.LF) is False
