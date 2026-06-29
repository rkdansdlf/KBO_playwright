"""Unit tests for calculate_standings pure functions."""

from __future__ import annotations

from datetime import date

import pytest

from src.cli.calculate_standings import (
    calculate_games_behind,
    iso_week_number,
)


class TestCalculateGamesBehind:
    def test_leader(self) -> None:
        assert calculate_games_behind(100, 50, 100, 50) == 0.0

    def test_one_game_behind(self) -> None:
        assert calculate_games_behind(99, 50, 100, 50) == 0.5

    def test_two_games_behind(self) -> None:
        assert calculate_games_behind(98, 50, 100, 50) == 1.0

    def test_mixed(self) -> None:
        assert calculate_games_behind(100, 52, 102, 50) == 2.0

    def test_zero_games(self) -> None:
        assert calculate_games_behind(0, 0, 0, 0) == 0.0


class TestIsoWeekNumber:
    def test_first_week(self) -> None:
        result = iso_week_number(date(2025, 1, 1))
        assert result.startswith("2025-W")

    def test_mid_year(self) -> None:
        result = iso_week_number(date(2025, 6, 15))
        assert result.startswith("2025-W")

    def test_end_of_year(self) -> None:
        result = iso_week_number(date(2025, 12, 31))
        assert "W" in result

    def test_format(self) -> None:
        result = iso_week_number(date(2025, 3, 15))
        assert len(result) == 8
        assert result[4] == "-"
        assert result[5] == "W"
