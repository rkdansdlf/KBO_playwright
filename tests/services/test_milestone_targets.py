"""Unit tests for initial_milestone_target."""

from __future__ import annotations

import pytest

from src.services.milestone_targets import initial_milestone_target


@pytest.mark.parametrize(
    ("category", "current_val", "expected"),
    [
        ("2000안타", 1990, 2000),
        ("300세이브", 280, 300),
        ("1,000탈삼진", 950, 1000),
        (" 10,000 타석 ", 9000, 10000),
    ],
)
def test_category_starting_with_integer(category: str, current_val: int, expected: int) -> None:
    """When category starts with an integer, return that integer (commas stripped)."""
    assert initial_milestone_target(category, current_val) == expected


@pytest.mark.parametrize(
    ("current_val", "expected"),
    [
        (-10, 10),
        (-5, 10),
        (-1, 10),
        (0, 10),
        (5, 10),
        (10, 20),
        (87, 90),
        (90, 100),
        (99, 100),
    ],
)
def test_round_milestone_under_100(current_val: int, expected: int) -> None:
    """For non-integer category with current_val < 100, step is 10 (0 or negative -> 10)."""
    assert initial_milestone_target("통산 안타", current_val) == expected


@pytest.mark.parametrize(
    ("current_val", "expected"),
    [
        (100, 150),
        (351, 400),
        (400, 450),
        (950, 1000),
        (1000, 1500),
        (1990, 2000),
        (2000, 2500),
        (2500, 3000),
    ],
)
def test_round_milestone_100_and_above(current_val: int, expected: int) -> None:
    """For non-integer category with current_val >= 100, step is 10**(digits-1)//2."""
    assert initial_milestone_target("통산 안타", current_val) == expected
