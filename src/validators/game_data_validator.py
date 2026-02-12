"""
Validation helpers for parsed game detail payloads.
"""
from __future__ import annotations

from typing import Dict, Any, List, Tuple


def validate_game_data(game_data: Dict[str, Any]) -> Tuple[bool, List[str], List[str]]:
    """
    Validate parsed game data prior to persistence.

    Returns:
        (is_valid, errors, warnings)
        - errors: Critical issues that prevent saving
        - warnings: Non-critical issues that should be logged
    """
    errors: List[str] = []
    warnings: List[str] = []

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
            # Check if it's a known international code (warnings only)
            # For now, following user request to enforce standard 10
            errors.append(f"Invalid {side} team code: '{code}'. Must be one of {sorted(list(STANDARD_TEAM_CODES))}")


    hitters = game_data.get("hitters") or {}
    pitchers = game_data.get("pitchers") or {}

    for side in ("home", "away"):
        if not hitters.get(side):
            errors.append(f"No hitter rows for {side}")
        if not pitchers.get(side):
            errors.append(f"No pitcher rows for {side}")

    # Non-critical validations (warnings only)
    _validate_score_totals(home, "home", warnings)
    _validate_score_totals(away, "away", warnings)
    _validate_runs_match_scores(game_data, warnings)

    return (len(errors) == 0, errors, warnings)


def _validate_score_totals(team: Dict[str, Any], label: str, errors: List[str]) -> None:
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


def _validate_runs_match_scores(game_data: Dict[str, Any], errors: List[str]) -> None:
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
            errors.append(
                f"{side} hitter runs ({total_runs}) != team score ({team['score']})"
            )


__all__ = ["validate_game_data"]

