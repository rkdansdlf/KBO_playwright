"""Unit tests for game_deduplication_service pure functions."""

from __future__ import annotations

import pytest

from src.services.game_deduplication_service import _select_primary


class TestSelectPrimary:
    def test_single_candidate(self) -> None:
        result = _select_primary([("G1", 100)], [])
        assert result == "G1"

    def test_higher_score_wins(self) -> None:
        result = _select_primary([("G1", 100), ("G2", 200)], [])
        assert result == "G2"

    def test_preferred_code_wins(self) -> None:
        result = _select_primary([("G1", 100), ("G2", 100)], ["G2"])
        assert result == "G2"

    def test_longer_code_wins(self) -> None:
        result = _select_primary([("G1", 100), ("G22", 100)], [])
        assert result == "G22"

    def test_alphabetical_wins(self) -> None:
        result = _select_primary([("GB", 100), ("GA", 100)], [])
        assert result == "GB"
