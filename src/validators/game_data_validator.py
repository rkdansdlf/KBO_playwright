"""Validation helpers for parsed game detail payloads."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.validators.stat_validator import StatValidator, ValidationResult


def _validate_required_rows(
    hitters: dict[str, Any],
    pitchers: dict[str, Any],
    errors: list[str],
) -> None:
    """Record missing hitter and pitcher rows for each team."""
    for side in ("home", "away"):
        if not hitters.get(side):
            errors.append(f"No hitter rows for {side}")
        if not pitchers.get(side):
            errors.append(f"No pitcher rows for {side}")


def validate_game_data(
    game_data: dict[str, Any],
    *,
    allow_partial: bool = False,
) -> tuple[bool, list[str], list[str]]:
    """Validate parsed game data prior to persistence.

    Args:
        game_data: Game Data.
        allow_partial: Allow anchored partial recovery payloads.

    Returns:
        (is_valid, errors, warnings)
        - errors: Critical issues that prevent saving
        - warnings: Non-critical issues that should be logged

    """
    errors: list[str] = []

    warnings: list[str] = []

    # Critical validations (will block save)
    if not game_data.get("game_id"):
        errors.append("Missing game_id")

    if not game_data.get("game_date"):
        errors.append("Missing game_date")

    teams = game_data.get("teams") or {}
    home = teams.get("home") or {}
    away = teams.get("away") or {}

    home_code = home.get("code")
    away_code = away.get("code")

    if not home_code:
        errors.append("Missing home team code")
    if not away_code:
        errors.append("Missing away team code")

    # Team code standardization check
    from src.utils.team_codes import STANDARD_TEAM_CODES

    for side, code in [("home", home_code), ("away", away_code)]:
        if code and code not in STANDARD_TEAM_CODES:
            errors.append(f"Invalid {side} team code: '{code}'. Must be one of {sorted(STANDARD_TEAM_CODES)}")

    hitters = game_data.get("hitters") or {}
    pitchers = game_data.get("pitchers") or {}
    metadata = game_data.get("metadata") or {}
    is_cancelled = bool(metadata.get("is_cancelled")) or game_data.get("game_status") in ("CANCELED", "CANCELLED")

    if not allow_partial and not is_cancelled:
        _validate_required_rows(hitters, pitchers, errors)
    elif not is_cancelled and not any(hitters.get(side) or pitchers.get(side) for side in ("home", "away")):
        has_anchor = bool(home_code and away_code) and any(
            team.get("score") is not None or team.get("line_score") or team.get("stadium") for team in (home, away)
        )
        if not has_anchor:
            errors.append("No detail rows for partial recovery")

    # Non-critical validations (warnings only)
    _validate_score_totals(home, "home", warnings)
    _validate_score_totals(away, "away", warnings)
    _validate_runs_match_scores(game_data, warnings)

    return (len(errors) == 0, errors, warnings)


def _validate_score_totals(team: dict[str, Any], label: str, errors: list[str]) -> None:
    score = team.get("score")
    line_score = team.get("line_score")
    if score is None or not line_score:
        return
    try:
        computed = sum(int(value or 0) for value in line_score)
    except ValueError:
        return
    if score != computed:
        errors.append(f"{label} line score ({computed}) != total score ({score})")


def _validate_runs_match_scores(game_data: dict[str, Any], errors: list[str]) -> None:
    teams = game_data.get("teams") or {}
    hitters = game_data.get("hitters") or {}
    for side in ("home", "away"):
        team = teams.get(side) or {}
        if team.get("score") is None:
            continue
        total_runs = 0
        for entry in hitters.get(side, []):
            stats = entry.get("stats") or {}
            total_runs += int(stats.get("runs") or 0)
        if total_runs != team["score"]:
            errors.append(f"{side} hitter runs ({total_runs}) != team score ({team['score']})")


def validate_game_detail_comprehensive(
    game_data: dict[str, Any],
    *,
    allow_partial: bool = False,
    stat_validator: StatValidator | None = None,
) -> tuple[bool, list[ValidationResult]]:
    """Perform comprehensive game, batting, and pitching validations.

    Returns:
        (is_valid, validation_results)

    """
    from src.validators.rules import create_default_stat_validator
    from src.validators.stat_validator import ValidationResult, ValidationSeverity

    validator = stat_validator or create_default_stat_validator()
    game_id = str(game_data.get("game_id") or "")

    # 1. Run baseline structural checks
    _, struct_errors, struct_warnings = validate_game_data(
        game_data,
        allow_partial=allow_partial,
    )
    results: list[ValidationResult] = [
        ValidationResult(
            validator="game_data_validator",
            rule_id="STRUCT-001",
            entity_type="game",
            field_name="structure",
            expected="Valid structure",
            actual=err,
            severity=ValidationSeverity.ERROR,
            game_id=game_id,
            message=err,
        )
        for err in struct_errors
    ]
    results.extend(
        ValidationResult(
            validator="game_data_validator",
            rule_id="STRUCT-002",
            entity_type="game",
            field_name="score_consistency",
            expected="Consistent scores",
            actual=warn,
            severity=ValidationSeverity.WARNING,
            game_id=game_id,
            message=warn,
        )
        for warn in struct_warnings
    )

    # 2. Run stat-level batting checks
    hitters = game_data.get("hitters") or {}
    for side in ("home", "away"):
        for entry in hitters.get(side, []):
            stats = dict(entry.get("stats") or {})
            stats["player_id"] = entry.get("player_id")
            stats["player_name"] = entry.get("player_name") or entry.get("name")
            batting_res = validator.validate_batting(stats, context={"game_id": game_id, "side": side})
            results.extend(batting_res)

    # 3. Run stat-level pitching checks
    pitchers = game_data.get("pitchers") or {}
    for side in ("home", "away"):
        for entry in pitchers.get(side, []):
            stats = dict(entry.get("stats") or {})
            stats["player_id"] = entry.get("player_id")
            stats["player_name"] = entry.get("player_name") or entry.get("name")
            pitching_res = validator.validate_pitching(stats, context={"game_id": game_id, "side": side})
            results.extend(pitching_res)

    has_blocking_error = any(r.is_blocking for r in results)
    return (not has_blocking_error, results)


__all__ = ["validate_game_data", "validate_game_detail_comprehensive"]
