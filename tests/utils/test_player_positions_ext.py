from src.utils.player_positions import (
    PositionCode,
    get_primary_position,
    is_battery,
    is_infield,
    is_outfield,
    normalize_position,
)


class TestNormalizePosition:
    def test_none_returns_empty(self):
        assert normalize_position(None) == []

    def test_empty_string_returns_empty(self):
        assert normalize_position("") == []

    def test_dash_returns_empty(self):
        assert normalize_position("-") == []

    def test_single_char_position(self):
        assert normalize_position("중") == [PositionCode.CF]
        assert normalize_position("투") == [PositionCode.P]
        assert normalize_position("포") == [PositionCode.C]

    def test_composite_position(self):
        assert normalize_position("타一") == [PositionCode.PH, PositionCode.B1]
        assert normalize_position("주二") == [PositionCode.PR, PositionCode.B2]
        assert normalize_position("유三") == [PositionCode.SS, PositionCode.B3]

    def test_numeric_code(self):
        assert normalize_position("6") == [PositionCode.SS]
        assert normalize_position("4") == [PositionCode.B2]

    def test_unknown_char_returns_unknown(self):
        assert normalize_position("X") == [PositionCode.UNKNOWN]


class TestGetPrimaryPosition:
    def test_last_position_wins(self):
        assert get_primary_position("타一") == PositionCode.B1
        assert get_primary_position("주二") == PositionCode.B2
        assert get_primary_position("유三") == PositionCode.B3

    def test_single_position(self):
        assert get_primary_position("중") == PositionCode.CF

    def test_empty_returns_unknown(self):
        assert get_primary_position("") == PositionCode.UNKNOWN


class TestPositionClassification:
    def test_is_infield(self):
        assert is_infield(PositionCode.SS) is True
        assert is_infield(PositionCode.B1) is True
        assert is_infield(PositionCode.CF) is False

    def test_is_outfield(self):
        assert is_outfield(PositionCode.CF) is True
        assert is_outfield(PositionCode.LF) is True
        assert is_outfield(PositionCode.P) is False

    def test_is_battery(self):
        assert is_battery(PositionCode.P) is True
        assert is_battery(PositionCode.C) is True
        assert is_battery(PositionCode.SS) is False

    def test_position_code_values(self):
        assert PositionCode.P.value == "P"
        assert PositionCode.DH.value == "DH"
        assert PositionCode.UNKNOWN.value == "UNKNOWN"
