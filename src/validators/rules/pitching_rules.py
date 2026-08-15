"""Pitching validation rules for KBO data pipeline."""

from __future__ import annotations

from typing import Any

from src.validators.stat_validator import ValidationResult, ValidationSeverity


def _get_int(record: dict[str, Any], key: str, default: int = 0) -> int:
    val = record.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _get_float(record: dict[str, Any], key: str) -> float | None:
    val = record.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def validate_earned_runs_le_runs(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """PIT-001: Earned Runs (ER) must be <= Runs Allowed (R)."""
    er = _get_int(record, "earned_runs")
    r = _get_int(record, "runs_allowed")
    game_id = context.get("game_id") if context else record.get("game_id")
    player_id = record.get("player_id") or record.get("player_name")

    if er > r:
        return [
            ValidationResult(
                validator="pitching_rules",
                rule_id="PIT-001",
                entity_type="pitching",
                field_name="earned_runs",
                expected=f"<= runs_allowed ({r})",
                actual=er,
                severity=ValidationSeverity.ERROR,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"Earned Runs ({er}) exceeds total Runs Allowed ({r})",
            ),
        ]
    return []


def validate_hits_allowed_le_batters_faced(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """PIT-002: Hits Allowed must be <= Batters Faced (TBF/BF) when BF > 0."""
    hits = _get_int(record, "hits_allowed")
    bf = _get_int(record, "batters_faced")
    game_id = context.get("game_id") if context else record.get("game_id")
    player_id = record.get("player_id") or record.get("player_name")

    if bf > 0 and hits > bf:
        return [
            ValidationResult(
                validator="pitching_rules",
                rule_id="PIT-002",
                entity_type="pitching",
                field_name="hits_allowed",
                expected=f"<= batters_faced ({bf})",
                actual=hits,
                severity=ValidationSeverity.ERROR,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"Hits Allowed ({hits}) exceeds Batters Faced ({bf})",
            ),
        ]
    return []


def validate_walks_allowed_le_batters_faced(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """PIT-003: Walks Allowed must be <= Batters Faced (BF) when BF > 0."""
    bb = _get_int(record, "walks_allowed")
    bf = _get_int(record, "batters_faced")
    game_id = context.get("game_id") if context else record.get("game_id")
    player_id = record.get("player_id") or record.get("player_name")

    if bf > 0 and bb > bf:
        return [
            ValidationResult(
                validator="pitching_rules",
                rule_id="PIT-003",
                entity_type="pitching",
                field_name="walks_allowed",
                expected=f"<= batters_faced ({bf})",
                actual=bb,
                severity=ValidationSeverity.ERROR,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"Walks Allowed ({bb}) exceeds Batters Faced ({bf})",
            ),
        ]
    return []


def validate_hr_allowed_le_hits_allowed(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """PIT-004: Home Runs Allowed must be <= Hits Allowed."""
    hr = _get_int(record, "home_runs_allowed")
    hits = _get_int(record, "hits_allowed")
    game_id = context.get("game_id") if context else record.get("game_id")
    player_id = record.get("player_id") or record.get("player_name")

    if hr > hits:
        return [
            ValidationResult(
                validator="pitching_rules",
                rule_id="PIT-004",
                entity_type="pitching",
                field_name="home_runs_allowed",
                expected=f"<= hits_allowed ({hits})",
                actual=hr,
                severity=ValidationSeverity.ERROR,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"Home Runs Allowed ({hr}) exceeds total Hits Allowed ({hits})",
            ),
        ]
    return []


def validate_innings_outs_consistency(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """PIT-005: Inning Outs vs IP decimal representation consistency."""
    outs = record.get("innings_outs")
    ip = record.get("innings_pitched")
    if outs is None or ip is None:
        return []

    outs_val = _get_int(record, "innings_outs")
    try:
        ip_float = float(ip)
    except (ValueError, TypeError):
        return []

    # IP can be represented as full innings + fractional (e.g. 5.1 -> 5 innings + 1 out = 16 outs, or 5.333)
    # Check if outs matches (floor(ip)*3 + remainder)
    full_innings = int(ip_float)
    decimal_part = round(ip_float - full_innings, 3)

    if decimal_part in (0.1, 0.333):
        expected_outs = (full_innings * 3) + 1
    elif decimal_part in (0.2, 0.667):
        expected_outs = (full_innings * 3) + 2
    elif decimal_part == 0.0:
        expected_outs = full_innings * 3
    else:
        expected_outs = None

    if expected_outs is not None and abs(outs_val - expected_outs) > 0:
        game_id = context.get("game_id") if context else record.get("game_id")
        player_id = record.get("player_id") or record.get("player_name")
        return [
            ValidationResult(
                validator="pitching_rules",
                rule_id="PIT-005",
                entity_type="pitching",
                field_name="innings_outs",
                expected=expected_outs,
                actual=outs_val,
                severity=ValidationSeverity.WARNING,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"Innings Outs ({outs_val}) != expected outs ({expected_outs}) for IP ({ip})",
            ),
        ]
    return []


ALL_PITCHING_RULES = [
    validate_earned_runs_le_runs,
    validate_hits_allowed_le_batters_faced,
    validate_walks_allowed_le_batters_faced,
    validate_hr_allowed_le_hits_allowed,
    validate_innings_outs_consistency,
]
