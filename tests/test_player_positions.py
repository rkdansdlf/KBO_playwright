import pytest
from src.utils.player_positions import normalize_position, get_primary_position, PositionCode

@pytest.mark.parametrize("raw, expected", [
    ("중", [PositionCode.CF]),
    ("좌", [PositionCode.LF]),
    ("우", [PositionCode.RF]),
    ("一", [PositionCode.B1]),
    ("二", [PositionCode.B2]),
    ("三", [PositionCode.B3]),
    ("유", [PositionCode.SS]),
    ("포", [PositionCode.C]),
    ("지", [PositionCode.DH]),
    ("투", [PositionCode.P]),
    ("타", [PositionCode.PH]),
    ("주", [PositionCode.PR]),
])
def test_normalize_simple(raw, expected):
    assert normalize_position(raw) == expected

@pytest.mark.parametrize("raw, expected", [
    ("타一", [PositionCode.PH, PositionCode.B1]),
    ("주二", [PositionCode.PR, PositionCode.B2]),
    ("유三", [PositionCode.SS, PositionCode.B3]),
    ("지우", [PositionCode.DH, PositionCode.RF]),
    ("타포", [PositionCode.PH, PositionCode.C]),
    ("주중", [PositionCode.PR, PositionCode.CF]),
    ("좌一", [PositionCode.LF, PositionCode.B1]),
])
def test_normalize_composite(raw, expected):
    assert normalize_position(raw) == expected

@pytest.mark.parametrize("raw, expected", [
    ("타一", PositionCode.B1),
    ("주二", PositionCode.B2),
    ("유三", PositionCode.B3),
    ("중", PositionCode.CF),
    ("타", PositionCode.PH),
    ("주", PositionCode.PR),
])
def test_get_primary_position(raw, expected):
    assert get_primary_position(raw) == expected

def test_normalize_edge_cases():
    assert normalize_position("") == []
    assert normalize_position(None) == []
    assert normalize_position("-") == []
    assert normalize_position("Unknown") == [PositionCode.UNKNOWN]

def test_complex_composite():
    # Example: '주좌一' (Pinch runner at 1st base who stayed as LF) - hypothetical but possible structure
    # Actually in KBO '주좌' might mean PR who moved to LF.
    assert normalize_position("주좌") == [PositionCode.PR, PositionCode.LF]
    assert get_primary_position("주좌") == PositionCode.LF
