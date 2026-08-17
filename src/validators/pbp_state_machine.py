"""Play-by-Play (PBP) State Machine & Cross-Reconciliation Validator.

This module implements a rigorous baseball state machine validator to audit
PBP event streams and cross-reconcile with boxscore tables (batting, pitching, scoreboard).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from src.validators.stat_validator import ValidationResult, ValidationSeverity

if TYPE_CHECKING:
    from collections.abc import Sequence

_MAX_LEGAL_OUTS = 3
_MAX_HALF_START_OUTS = 2


@dataclass(frozen=True)
class PBPStateMachineReport:
    """Report summarizing PBP state machine & reconciliation findings."""

    game_id: str
    total_events: int
    is_valid: bool
    violations: list[ValidationResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "game_id": self.game_id,
            "total_events": self.total_events,
            "is_valid": self.is_valid,
            "violations": [
                {
                    "rule_id": v.rule_id,
                    "severity": v.severity.value,
                    "field_name": v.field_name,
                    "expected": v.expected,
                    "actual": v.actual,
                    "message": v.message,
                }
                for v in self.violations
            ],
        }


def _normalize_half(half: object) -> str:
    """Normalize inning half string/int representation to 'TOP' or 'BOT'."""
    if half is None:
        return "TOP"
    s = str(half).strip().upper()
    if s in ("TOP", "T", "0", "초", "TOP_OF_INNING"):
        return "TOP"
    if s in ("BOT", "BOTTOM", "B", "1", "말", "BOTTOM_OF_INNING"):
        return "BOT"
    return s


def _check_score_and_out_bounds(
    g_id: str,
    seq: int,
    outs: int,
    scores: tuple[int, int],  # (away_score, home_score)
    prev_scores: tuple[int, int],  # (prev_away, prev_home)
) -> list[ValidationResult]:
    """Validate that outs are within 0-3 and scores never decrease."""
    res: list[ValidationResult] = []
    away_score, home_score = scores
    prev_away, prev_home = prev_scores

    if outs < 0 or outs > _MAX_LEGAL_OUTS:
        res.append(
            ValidationResult(
                validator="pbp_state_machine",
                rule_id="PBP-004",
                entity_type="pbp",
                field_name="outs",
                expected="0 <= outs <= 3",
                actual=outs,
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message=f"Invalid out count {outs} at seq {seq}",
            ),
        )
    if away_score < prev_away:
        res.append(
            ValidationResult(
                validator="pbp_state_machine",
                rule_id="PBP-005",
                entity_type="pbp",
                field_name="away_score",
                expected=f">= {prev_away}",
                actual=away_score,
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message=f"Away score decreased from {prev_away} to {away_score} at seq {seq}",
            ),
        )
    if home_score < prev_home:
        res.append(
            ValidationResult(
                validator="pbp_state_machine",
                rule_id="PBP-006",
                entity_type="pbp",
                field_name="home_score",
                expected=f">= {prev_home}",
                actual=home_score,
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message=f"Home score decreased from {prev_home} to {home_score} at seq {seq}",
            ),
        )
    return res


def validate_pbp_state_machine(
    events: Sequence[dict[str, Any]],
    *,
    game_id: str | None = None,
    expected_final_score: tuple[int, int] | None = None,
) -> PBPStateMachineReport:
    """Validate PBP events against baseball state machine rules."""
    g_id = str(game_id or (events[0].get("game_id") if events else "UNKNOWN"))
    violations: list[ValidationResult] = []

    if not events:
        violations.append(
            ValidationResult(
                validator="pbp_state_machine",
                rule_id="PBP-000",
                entity_type="pbp",
                field_name="events",
                expected="> 0 events",
                actual=0,
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message="No PBP events found for completed game",
            ),
        )
        return PBPStateMachineReport(game_id=g_id, total_events=0, is_valid=False, violations=violations)

    prev_inning, prev_half = 1, "TOP"
    prev_away, prev_home = 0, 0
    seen_seqs: set[int] = set()

    for idx, ev in enumerate(events):
        seq = int(ev.get("event_seq", idx + 1))
        inning = int(ev.get("inning", 1))
        half = _normalize_half(ev.get("half"))
        outs = int(ev.get("outs", ev.get("out_count", 0)) or 0)
        away_score = int(ev.get("away_score", ev.get("score_away", 0)) or 0)
        home_score = int(ev.get("home_score", ev.get("score_home", 0)) or 0)

        if seq in seen_seqs:
            violations.append(
                ValidationResult(
                    validator="pbp_state_machine",
                    rule_id="PBP-001",
                    entity_type="pbp",
                    field_name="event_seq",
                    expected="unique sequence",
                    actual=seq,
                    severity=ValidationSeverity.ERROR,
                    game_id=g_id,
                    message=f"Duplicate event sequence {seq} at index {idx}",
                ),
            )
        seen_seqs.add(seq)

        if inning < prev_inning:
            violations.append(
                ValidationResult(
                    validator="pbp_state_machine",
                    rule_id="PBP-002",
                    entity_type="pbp",
                    field_name="inning",
                    expected=f">= {prev_inning}",
                    actual=inning,
                    severity=ValidationSeverity.ERROR,
                    game_id=g_id,
                    message=f"Inning reverted from {prev_inning} to {inning} at seq {seq}",
                ),
            )

        if (inning != prev_inning or half != prev_half) and idx > 0 and outs > _MAX_HALF_START_OUTS:
            violations.append(
                ValidationResult(
                    validator="pbp_state_machine",
                    rule_id="PBP-003",
                    entity_type="pbp",
                    field_name="outs",
                    expected="reset to 0 on half transition",
                    actual=outs,
                    severity=ValidationSeverity.WARNING,
                    game_id=g_id,
                    message=f"Outs not reset on new half-inning {inning} {half} at seq {seq}",
                ),
            )

        violations.extend(
            _check_score_and_out_bounds(
                g_id,
                seq,
                outs,
                (away_score, home_score),
                (prev_away, prev_home),
            ),
        )

        prev_inning, prev_half = inning, half
        prev_away = max(prev_away, away_score)
        prev_home = max(prev_home, home_score)

    if expected_final_score is not None:
        exp_away, exp_home = expected_final_score
        if (prev_away, prev_home) != (exp_away, exp_home):
            violations.append(
                ValidationResult(
                    validator="pbp_state_machine",
                    rule_id="PBP-007",
                    entity_type="pbp",
                    field_name="final_score",
                    expected=f"({exp_away}, {exp_home})",
                    actual=f"({prev_away}, {prev_home})",
                    severity=ValidationSeverity.ERROR,
                    game_id=g_id,
                    message=f"PBP final ({prev_away}, {prev_home}) != score ({exp_away}, {exp_home})",
                ),
            )

    is_valid = not any(v.severity == ValidationSeverity.ERROR for v in violations)
    return PBPStateMachineReport(game_id=g_id, total_events=len(events), is_valid=is_valid, violations=violations)


def reconcile_pbp_with_boxscore(
    pbp_events: Sequence[dict[str, Any]],
    boxscore_batting: Sequence[dict[str, Any]],
    boxscore_pitching: Sequence[dict[str, Any]],
    *,
    game_id: str | None = None,
    scoreboard_runs: tuple[int, int] | None = None,
) -> list[ValidationResult]:
    """Cross-reconcile PBP derived totals against Boxscore batting/pitching and Scoreboard."""
    g_id = str(game_id or (pbp_events[0].get("game_id") if pbp_events else "UNKNOWN"))
    results: list[ValidationResult] = []

    batting_so = sum(int(b.get("strikeouts", 0) or 0) for b in boxscore_batting)
    pitching_so = sum(int(p.get("strikeouts", 0) or 0) for p in boxscore_pitching)

    if batting_so != pitching_so and (batting_so > 0 or pitching_so > 0):
        results.append(
            ValidationResult(
                validator="pbp_reconciliation",
                rule_id="REC-001",
                entity_type="boxscore",
                field_name="strikeouts",
                expected=f"pitching SO ({pitching_so})",
                actual=batting_so,
                severity=ValidationSeverity.ERROR,
                game_id=g_id,
                message=f"Total batting SO ({batting_so}) != total pitching SO ({pitching_so})",
            ),
        )

    if scoreboard_runs is not None:
        total_scoreboard_runs = scoreboard_runs[0] + scoreboard_runs[1]
        batting_runs = sum(int(b.get("runs", 0) or 0) for b in boxscore_batting)
        pitching_runs = sum(int(p.get("runs_allowed", 0) or 0) for p in boxscore_pitching)

        if batting_runs != total_scoreboard_runs:
            results.append(
                ValidationResult(
                    validator="pbp_reconciliation",
                    rule_id="REC-002",
                    entity_type="boxscore",
                    field_name="runs",
                    expected=f"scoreboard total ({total_scoreboard_runs})",
                    actual=batting_runs,
                    severity=ValidationSeverity.ERROR,
                    game_id=g_id,
                    message=f"Batting runs ({batting_runs}) != scoreboard runs ({total_scoreboard_runs})",
                ),
            )
        if pitching_runs != total_scoreboard_runs:
            results.append(
                ValidationResult(
                    validator="pbp_reconciliation",
                    rule_id="REC-003",
                    entity_type="boxscore",
                    field_name="runs_allowed",
                    expected=f"scoreboard total ({total_scoreboard_runs})",
                    actual=pitching_runs,
                    severity=ValidationSeverity.ERROR,
                    game_id=g_id,
                    message=f"Pitcher runs ({pitching_runs}) != scoreboard runs ({total_scoreboard_runs})",
                ),
            )

    return results
