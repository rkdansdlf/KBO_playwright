"""Unit tests for player_positions pure functions."""

from __future__ import annotations

import pytest

from src.utils.player_positions import (
    PositionCode,
    get_primary_position,
    is_battery,
    is_infield,
    is_outfield,
    normalize_position,
)


class TestNormalizePosition:
    def test_single(self) -> None:
        result = normalize_position("투수")
        assert len(result) == 1

    def test_none(self) -> None:
        result = normalize_position(None)
        assert result == []

    def test_empty(self) -> None:
        result = normalize_position("")
        assert result == []


class TestGetPrimaryPosition:
    def test_pitcher(self) -> None:
        assert get_primary_position("투수") == PositionCode.P

    def test_none(self) -> None:
        result = get_primary_position(None)
        assert result == PositionCode.UNKNOWN

    def test_empty(self) -> None:
        result = get_primary_position("")
        assert result == PositionCode.UNKNOWN


class TestIsInfield:
    def test_infield(self) -> None:
        assert is_infield(PositionCode.SS) is True

    def test_pitcher(self) -> None:
        assert is_infield(PositionCode.P) is False


class TestIsOutfield:
    def test_outfield(self) -> None:
        assert is_outfield(PositionCode.LF) is True

    def test_pitcher(self) -> None:
        assert is_outfield(PositionCode.P) is False


class TestIsBattery:
    def test_battery(self) -> None:
        assert is_battery(PositionCode.C) is True

    def test_pitcher_is_battery(self) -> None:
        assert is_battery(PositionCode.P) is True

    def test_infield_not_battery(self) -> None:
        assert is_battery(PositionCode.SS) is False
