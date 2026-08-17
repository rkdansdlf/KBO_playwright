"""Appearance-Type Aware Lineup Validation Rules.

Validates that players in game lineups have corresponding game batting or pitching
records according to their appearance role (STARTER, PH, PR, DEF_SUB, PITCHER).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.validators.stat_validator import ValidationResult, ValidationSeverity

if TYPE_CHECKING:
    from collections.abc import Sequence

_MIN_BATTING_ORDER = 1
_MAX_STARTER_BATTING_ORDER = 9
_STARTER_SUB_ORDER = 1


_POS_TO_APP_TYPE = {
    "P": "PITCHER",
    "투수": "PITCHER",
    "PITCHER": "PITCHER",
    "대주자": "PR",
    "PR": "PR",
    "PINCH_RUNNER": "PR",
    "대수비": "DEF_SUB",
    "DEF": "DEF_SUB",
    "DEF_SUB": "DEF_SUB",
    "DEFENSIVE_SUB": "DEF_SUB",
    "대타": "PH",
    "PH": "PH",
    "PINCH_HITTER": "PH",
}


def classify_appearance_type(row: dict[str, Any]) -> str:
    """Classify a lineup player's appearance type."""
    pos = str(row.get("position") or row.get("standard_position", "")).strip().upper()
    batting_order = row.get("batting_order")
    is_starter = row.get("is_starter")
    appearance_seq = row.get("appearance_seq", row.get("sub_order", 1))

    if pos in ("P", "투수", "PITCHER") or batting_order in (0, "0", None):
        return "PITCHER"

    try:
        bo_int = int(batting_order)
    except (ValueError, TypeError):
        bo_int = 0

    if is_starter in (1, True, "1", "TRUE"):
        return "STARTER"

    if is_starter is None and _MIN_BATTING_ORDER <= bo_int <= _MAX_STARTER_BATTING_ORDER and appearance_seq in (1, "1"):
        return "STARTER"

    return _POS_TO_APP_TYPE.get(pos, "OTHER")


def validate_lineup_appearances(
    lineup_rows: Sequence[dict[str, Any]],
    batting_rows: Sequence[dict[str, Any]],
    pitching_rows: Sequence[dict[str, Any]],
    *,
    game_id: str | None = None,
) -> list[ValidationResult]:
    """Validate lineup records against batting and pitching appearances conditionally."""
    g_id = game_id or (lineup_rows[0].get("game_id") if lineup_rows else "UNKNOWN")
    results: list[ValidationResult] = []

    batting_player_ids = {str(b.get("player_id")) for b in batting_rows if b.get("player_id") is not None}
    pitching_player_ids = {str(p.get("player_id")) for p in pitching_rows if p.get("player_id") is not None}
    batting_player_names = {str(b.get("player_name")) for b in batting_rows if b.get("player_name")}
    pitching_player_names = {str(p.get("player_name")) for p in pitching_rows if p.get("player_name")}

    for row in lineup_rows:
        pid = str(row.get("player_id")) if row.get("player_id") is not None else None
        pname = str(row.get("player_name", ""))
        app_type = classify_appearance_type(row)

        has_batting = (pid in batting_player_ids) or (pname in batting_player_names)
        has_pitching = (pid in pitching_player_ids) or (pname in pitching_player_names)

        if app_type in ("STARTER", "PH") and not has_batting:
            results.append(
                ValidationResult(
                    validator="lineup_rules",
                    rule_id="LIN-001",
                    entity_type="lineup",
                    field_name="batting_record",
                    expected="present in player_game_batting",
                    actual="missing",
                    severity=ValidationSeverity.ERROR,
                    game_id=str(g_id),
                    entity_id=pid or pname,
                    message=f"{app_type} '{pname}' ({pid}) missing from player_game_batting",
                ),
            )
        elif app_type == "PITCHER" and not has_pitching:
            results.append(
                ValidationResult(
                    validator="lineup_rules",
                    rule_id="LIN-002",
                    entity_type="lineup",
                    field_name="pitching_record",
                    expected="present in player_game_pitching",
                    actual="missing",
                    severity=ValidationSeverity.ERROR,
                    game_id=str(g_id),
                    entity_id=pid or pname,
                    message=f"Pitcher '{pname}' ({pid}) missing from player_game_pitching",
                ),
            )
        elif app_type in ("PR", "DEF_SUB"):
            pass
        elif not (has_batting or has_pitching):
            results.append(
                ValidationResult(
                    validator="lineup_rules",
                    rule_id="LIN-003",
                    entity_type="lineup",
                    field_name="appearance",
                    expected="present in batting or pitching",
                    actual="missing in both",
                    severity=ValidationSeverity.WARNING,
                    game_id=str(g_id),
                    entity_id=pid or pname,
                    message=f"Lineup player '{pname}' ({pid}) of type {app_type} not found in batting or pitching",
                ),
            )

    return results
