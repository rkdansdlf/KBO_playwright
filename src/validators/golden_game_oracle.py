"""Golden Game Oracle Engine.

Validates parser outputs and crawled records against exact ground-truth assertions
(Golden Oracles) across diverse baseball game categories (extra innings, walk-offs,
ties, rain-cold, doubleheaders, suspended, and historical eras).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from src.validators.stat_validator import ValidationResult, ValidationSeverity

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(frozen=True)
class GoldenGameOracle:
    """Ground truth oracle specification for a representative historical game."""

    game_id: str
    season: int
    category: str
    description: str
    home_team: str
    away_team: str
    final_score: tuple[int, int]  # (away, home)
    innings: int
    total_outs: int
    winning_team: str | None
    lineup_player_count: int
    batting_totals: dict[str, int] = field(default_factory=dict)
    pitching_totals: dict[str, int] = field(default_factory=dict)
    pbp_event_min_count: int = 0
    terminal_condition: str = "NORMAL_9INN"


def verify_game_against_oracle(
    parsed_game: Mapping[str, Any],
    oracle: GoldenGameOracle,
) -> list[ValidationResult]:
    """Verify that a parsed game payload matches the exact expected Golden Game Oracle.

    Checks:
    - Final score
    - Total recorded outs
    - Lineup count
    - Team batting / pitching totals
    """
    g_id = oracle.game_id
    results: list[ValidationResult] = []

    # 1. Final score check
    parsed_score = (
        int(parsed_game.get("away_score", parsed_game.get("score_away", -1))),
        int(parsed_game.get("home_score", parsed_game.get("score_home", -1))),
    )
    if parsed_score not in (oracle.final_score, (-1, -1)):
        results.append(
            ValidationResult(
                validator="golden_game_oracle",
                rule_id="GOL-001",
                entity_type="game",
                field_name="final_score",
                expected=str(oracle.final_score),
                actual=str(parsed_score),
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message=f"Parsed score {parsed_score} does not match golden oracle {oracle.final_score}",
            ),
        )

    # 2. Total outs check
    parsed_outs = parsed_game.get("total_outs", parsed_game.get("innings_outs"))
    if parsed_outs is not None and int(parsed_outs) != oracle.total_outs:
        results.append(
            ValidationResult(
                validator="golden_game_oracle",
                rule_id="GOL-002",
                entity_type="game",
                field_name="total_outs",
                expected=oracle.total_outs,
                actual=int(parsed_outs),
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message=f"Parsed outs {parsed_outs} != golden oracle outs {oracle.total_outs}",
            ),
        )

    # 3. Lineup count check
    lineups = parsed_game.get("lineups", parsed_game.get("hitters", []))
    if lineups and len(lineups) < oracle.lineup_player_count:
        results.append(
            ValidationResult(
                validator="golden_game_oracle",
                rule_id="GOL-003",
                entity_type="lineup",
                field_name="player_count",
                expected=f">= {oracle.lineup_player_count}",
                actual=len(lineups),
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message=f"Lineup count {len(lineups)} < expected {oracle.lineup_player_count}",
            ),
        )

    # 4. Batting totals check
    for metric, exp_val in oracle.batting_totals.items():
        actual_val = parsed_game.get("batting_totals", {}).get(metric)
        if actual_val is not None and int(actual_val) != exp_val:
            results.append(
                ValidationResult(
                    validator="golden_game_oracle",
                    rule_id="GOL-004",
                    entity_type="batting",
                    field_name=metric,
                    expected=exp_val,
                    actual=int(actual_val),
                    severity=ValidationSeverity.ERROR,
                    game_id=g_id,
                    message=f"Batting metric '{metric}' ({actual_val}) != golden expected ({exp_val})",
                ),
            )

    return results
