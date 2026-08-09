"""Unit tests for Auto-Healer DB View discrepancy detection."""

from __future__ import annotations

from src.cli.auto_healer import _find_season_stat_discrepancies, heal_season_stat_discrepancies


def test_find_season_stat_discrepancies_return_type() -> None:
    """Test _find_season_stat_discrepancies returns a list."""
    res = _find_season_stat_discrepancies()
    assert isinstance(res, list)


def test_heal_season_stat_discrepancies_dry_run() -> None:
    """Test heal_season_stat_discrepancies in dry_run mode."""
    count = heal_season_stat_discrepancies(dry_run=True)
    assert isinstance(count, int)
