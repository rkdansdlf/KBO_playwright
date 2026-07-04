"""Unit tests for game_collection_service pure functions."""

from __future__ import annotations

import pytest

from src.services.game_collection_service import (
    build_game_id_range,
    normalize_game_targets,
)


class TestBuildGameIdRange:
    def test_full_year(self) -> None:
        start, end = build_game_id_range(2025, None)
        assert start == "20250101"
        assert end == "20260101"

    def test_january(self) -> None:
        start, end = build_game_id_range(2025, 1)
        assert start == "20250101"
        assert end == "20250201"

    def test_december(self) -> None:
        start, end = build_game_id_range(2025, 12)
        assert start == "20251201"
        assert end == "20260101"

    def test_june(self) -> None:
        start, end = build_game_id_range(2025, 6)
        assert start == "20250601"
        assert end == "20250701"


class TestNormalizeGameTargets:
    def test_empty(self) -> None:
        result = normalize_game_targets([])
        assert result == []

    def test_valid_game(self) -> None:
        games = [{"game_id": "20250601LGSS0", "game_date": "2025-06-01"}]
        result = normalize_game_targets(games)
        assert len(result) == 1
        assert result[0].game_id == "20250601LGSS0"

    def test_duplicate_removed(self) -> None:
        games = [
            {"game_id": "20250601LGSS0", "game_date": "2025-06-01"},
            {"game_id": "20250601LGSS0", "game_date": "2025-06-01"},
        ]
        result = normalize_game_targets(games)
        assert len(result) == 1

    def test_missing_game_id_skipped(self) -> None:
        games = [{"game_date": "2025-06-01"}]
        result = normalize_game_targets(games)
        assert result == []
