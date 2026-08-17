"""Tests for Appearance-Type Aware Lineup Validation Rules."""

from __future__ import annotations

import pytest

from src.validators.lineup_rules import (
    classify_appearance_type,
    validate_lineup_appearances,
)
from src.validators.stat_validator import ValidationSeverity


class TestLineupRules:
    def test_classify_appearance_types(self) -> None:
        assert classify_appearance_type({"batting_order": 1, "sub_order": 1, "position": "중견수"}) == "STARTER"
        assert classify_appearance_type({"batting_order": 0, "position": "투수"}) == "PITCHER"
        assert classify_appearance_type({"batting_order": 4, "sub_order": 2, "position": "대타"}) == "PH"
        assert classify_appearance_type({"batting_order": 4, "sub_order": 2, "position": "대주자"}) == "PR"
        assert classify_appearance_type({"batting_order": 4, "sub_order": 2, "position": "대수비"}) == "DEF_SUB"

    def test_valid_lineup_validation(self) -> None:
        lineup = [
            {"player_id": 101, "player_name": "홍길동", "batting_order": 1, "sub_order": 1, "position": "CF"},
            {"player_id": 201, "player_name": "김투수", "batting_order": 0, "sub_order": 1, "position": "P"},
            {"player_id": 301, "player_name": "이대주", "batting_order": 1, "sub_order": 2, "position": "대주자"},
        ]
        batting = [{"player_id": 101, "player_name": "홍길동"}]
        pitching = [{"player_id": 201, "player_name": "김투수"}]

        # Note: 301 (PR) has no batting row, but should NOT raise an error!
        results = validate_lineup_appearances(lineup, batting, pitching, game_id="20240501LGHH0")
        assert len(results) == 0

    def test_starter_missing_batting_detected(self) -> None:
        lineup = [
            {"player_id": 101, "player_name": "홍길동", "batting_order": 1, "sub_order": 1, "position": "CF"},
        ]
        batting: list[dict] = []
        pitching: list[dict] = []

        results = validate_lineup_appearances(lineup, batting, pitching, game_id="20240501LGHH0")
        assert len(results) == 1
        assert results[0].rule_id == "LIN-001"
        assert results[0].severity == ValidationSeverity.ERROR
        assert "STARTER '홍길동'" in results[0].message
