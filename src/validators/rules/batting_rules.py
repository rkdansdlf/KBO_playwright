"""Batting validation rules for KBO data pipeline."""

from __future__ import annotations

from typing import Any

from src.validators.stat_validator import ValidationResult, ValidationSeverity

_AVG_TOLERANCE = 0.006
_OBP_MIN_AT_BATS = 10
_OBP_DIFF_THRESHOLD = 0.015


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


def validate_hits_le_at_bats(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """BAT-001: Hits must be less than or equal to At-Bats."""
    hits = _get_int(record, "hits")
    at_bats = _get_int(record, "at_bats")
    game_id = context.get("game_id") if context else record.get("game_id")
    player_id = record.get("player_id") or record.get("player_name")

    if hits > at_bats:
        return [
            ValidationResult(
                validator="batting_rules",
                rule_id="BAT-001",
                entity_type="batting",
                field_name="hits",
                expected=f"<= at_bats ({at_bats})",
                actual=hits,
                severity=ValidationSeverity.ERROR,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"Hits ({hits}) exceeds At-Bats ({at_bats})",
            ),
        ]
    return []


def validate_extra_base_hits_le_hits(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """BAT-002: Doubles + Triples + Home Runs must be <= total Hits."""
    doubles = _get_int(record, "doubles")
    triples = _get_int(record, "triples")
    home_runs = _get_int(record, "home_runs")
    hits = _get_int(record, "hits")
    extra_bases_sum = doubles + triples + home_runs

    game_id = context.get("game_id") if context else record.get("game_id")
    player_id = record.get("player_id") or record.get("player_name")

    if extra_bases_sum > hits:
        return [
            ValidationResult(
                validator="batting_rules",
                rule_id="BAT-002",
                entity_type="batting",
                field_name="hits",
                expected=f">= extra_base_hits sum ({extra_bases_sum})",
                actual=hits,
                severity=ValidationSeverity.ERROR,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=(
                    f"Extra-base hits sum ({doubles} + {triples} + {home_runs} = {extra_bases_sum}) "
                    f"exceeds total Hits ({hits})"
                ),
            ),
        ]
    return []


def validate_total_bases_formula(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """BAT-003: Total Bases (TB) must equal Singles + 2*2B + 3*3B + 4*HR."""
    tb = record.get("total_bases")
    if tb is None:
        return []

    tb_val = _get_int(record, "total_bases")
    hits = _get_int(record, "hits")
    doubles = _get_int(record, "doubles")
    triples = _get_int(record, "triples")
    home_runs = _get_int(record, "home_runs")

    singles = hits - (doubles + triples + home_runs)
    if singles < 0:
        return []  # Caught by BAT-002

    expected_tb = singles + (2 * doubles) + (3 * triples) + (4 * home_runs)
    if tb_val != expected_tb:
        game_id = context.get("game_id") if context else record.get("game_id")
        player_id = record.get("player_id") or record.get("player_name")
        return [
            ValidationResult(
                validator="batting_rules",
                rule_id="BAT-003",
                entity_type="batting",
                field_name="total_bases",
                expected=expected_tb,
                actual=tb_val,
                severity=ValidationSeverity.ERROR,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"Total Bases ({tb_val}) != calculated TB ({expected_tb})",
            ),
        ]
    return []


def validate_pa_components_formula(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """BAT-004: Plate Appearances (PA) must equal AB + BB + HBP + SH + SF when all fields are present."""
    if "plate_appearances" not in record or record.get("plate_appearances") is None:
        return []

    pa = _get_int(record, "plate_appearances")
    ab = _get_int(record, "at_bats")
    bb = _get_int(record, "walks")
    hbp = _get_int(record, "hbp")
    sh = _get_int(record, "sacrifice_hits")
    sf = _get_int(record, "sacrifice_flies")

    expected_pa = ab + bb + hbp + sh + sf
    if pa != expected_pa and pa > 0:
        game_id = context.get("game_id") if context else record.get("game_id")
        player_id = record.get("player_id") or record.get("player_name")
        msg = f"PA ({pa}) != components sum ({ab}AB+{bb}BB+{hbp}HBP+{sh}SH+{sf}SF = {expected_pa})"
        return [
            ValidationResult(
                validator="batting_rules",
                rule_id="BAT-004",
                entity_type="batting",
                field_name="plate_appearances",
                expected=expected_pa,
                actual=pa,
                severity=ValidationSeverity.WARNING,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=msg,
            ),
        ]
    return []


def validate_avg_consistency(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """BAT-005: AVG consistency when both AB and AVG are provided."""
    avg = _get_float(record, "avg")
    if avg is None:
        return []

    ab = _get_int(record, "at_bats")
    hits = _get_int(record, "hits")

    if ab > 0:
        calculated_avg = hits / ab
        diff = abs(avg - calculated_avg)
        if diff > _AVG_TOLERANCE:
            game_id = context.get("game_id") if context else record.get("game_id")
            player_id = record.get("player_id") or record.get("player_name")
            return [
                ValidationResult(
                    validator="batting_rules",
                    rule_id="BAT-005",
                    entity_type="batting",
                    field_name="avg",
                    expected=round(calculated_avg, 3),
                    actual=avg,
                    severity=ValidationSeverity.WARNING,
                    game_id=str(game_id) if game_id else None,
                    entity_id=player_id,
                    message=f"Provided AVG ({avg:.3f}) differs from H/AB ({calculated_avg:.3f})",
                ),
            ]
    return []


def validate_obp_ge_avg(
    record: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> list[ValidationResult]:
    """BAT-006: OBP is mathematically expected to be >= AVG in typical sample size."""
    avg = _get_float(record, "avg")
    obp = _get_float(record, "obp")
    ab = _get_int(record, "at_bats")

    if avg is not None and obp is not None and ab >= _OBP_MIN_AT_BATS and obp < (avg - _OBP_DIFF_THRESHOLD):
        game_id = context.get("game_id") if context else record.get("game_id")
        player_id = record.get("player_id") or record.get("player_name")
        return [
            ValidationResult(
                validator="batting_rules",
                rule_id="BAT-006",
                entity_type="batting",
                field_name="obp",
                expected=f">= avg ({avg:.3f})",
                actual=obp,
                severity=ValidationSeverity.WARNING,
                game_id=str(game_id) if game_id else None,
                entity_id=player_id,
                message=f"OBP ({obp:.3f}) is unexpectedly lower than AVG ({avg:.3f}) for {ab} AB",
            ),
        ]
    return []


ALL_BATTING_RULES = [
    validate_hits_le_at_bats,
    validate_extra_base_hits_le_hits,
    validate_total_bases_formula,
    validate_pa_components_formula,
    validate_avg_consistency,
    validate_obp_ge_avg,
]
