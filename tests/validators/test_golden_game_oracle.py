"""Tests for Golden Game Oracle Verification."""

from __future__ import annotations

import pytest

from src.validators.golden_game_oracle import (
    GoldenGameOracle,
    verify_game_against_oracle,
)


class TestGoldenGameOracle:
    def test_exact_match_passes_cleanly(self) -> None:
        oracle = GoldenGameOracle(
            game_id="20240405LGKT0",
            season=2024,
            category="standard_9inn",
            description="Standard 9-inning regular season game",
            home_team="KT",
            away_team="LG",
            final_score=(7, 8),
            innings=9,
            total_outs=54,
            winning_team="KT",
            lineup_player_count=20,
            batting_totals={"hits": 15, "runs": 15},
        )
        parsed_game = {
            "away_score": 7,
            "home_score": 8,
            "total_outs": 54,
            "lineups": [{} for _ in range(22)],
            "batting_totals": {"hits": 15, "runs": 15},
        }
        violations = verify_game_against_oracle(parsed_game, oracle)
        assert len(violations) == 0

    def test_outs_and_batting_mismatch_detected(self) -> None:
        oracle = GoldenGameOracle(
            game_id="20230521SSOB0",
            season=2023,
            category="extra_innings_tie",
            description="12-inning tie game",
            home_team="OB",
            away_team="SS",
            final_score=(2, 2),
            innings=12,
            total_outs=72,
            winning_team=None,
            lineup_player_count=24,
            batting_totals={"hits": 14},
        )
        parsed_game = {
            "away_score": 2,
            "home_score": 2,
            "total_outs": 54,  # Mutated outs (expected 72 for 12 innings)
            "lineups": [{} for _ in range(24)],
            "batting_totals": {"hits": 10},  # Mutated hits
        }
        violations = verify_game_against_oracle(parsed_game, oracle)
        assert len(violations) == 2
        rule_ids = {v.rule_id for v in violations}
        assert "GOL-002" in rule_ids
        assert "GOL-004" in rule_ids
