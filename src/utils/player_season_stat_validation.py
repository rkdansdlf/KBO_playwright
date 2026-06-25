"""Validation helpers for player season stat payloads."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

from src.utils.player_validation import normalize_player_id, normalize_player_name, validate_player_payload

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

BATTING_CORE_STATS = ("games", "plate_appearances", "at_bats", "hits")
PITCHING_CORE_STATS = ("games", "innings_pitched", "innings_outs")
FIELDING_CORE_STATS = ("games", "innings", "putouts", "assists", "errors", "fielding_pct")

NUMERIC_FIELDS = {
    "player_id",
    "season",
    "year",
    "games",
    "plate_appearances",
    "at_bats",
    "runs",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "rbi",
    "walks",
    "intentional_walks",
    "hbp",
    "strikeouts",
    "stolen_bases",
    "caught_stealing",
    "sacrifice_hits",
    "sacrifice_flies",
    "gdp",
    "avg",
    "obp",
    "slg",
    "ops",
    "iso",
    "babip",
    "games_started",
    "wins",
    "losses",
    "saves",
    "holds",
    "innings_pitched",
    "innings_outs",
    "hits_allowed",
    "runs_allowed",
    "earned_runs",
    "home_runs_allowed",
    "walks_allowed",
    "hit_batters",
    "wild_pitches",
    "balks",
    "era",
    "whip",
    "fip",
    "k_per_nine",
    "bb_per_nine",
    "kbb",
    "complete_games",
    "shutouts",
    "quality_starts",
    "blown_saves",
    "tbf",
    "np",
    "avg_against",
    "doubles_allowed",
    "triples_allowed",
    "sacrifices_allowed",
    "sacrifice_flies_allowed",
    "innings",
    "putouts",
    "assists",
    "errors",
    "double_plays",
    "fielding_pct",
    "pickoffs",
}


def _is_number_like(value: object) -> bool:
    """Handles the is number like operation.

    Args:
        value: Value.

    Returns:
        True if the condition is met, False otherwise.

    """
    if value is None:
        return True
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned in {"", "-", "–", "—"}:
            return True
        try:
            float(cleaned)
        except ValueError:
            return False
        else:
            return True
    return False


def _number_or_none(value: object) -> float | None:
    """Handles the number or none operation.

    Args:
        value: Value.

    Returns:
        The result of the operation.

    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned in {"", "-", "–", "—"}:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _has_core_stats(payload: Mapping[str, Any], stat_type: str) -> bool:
    """Handles the has core stats operation.

    Args:
        payload: Data payload to process.
        stat_type: Stat Type.

    Returns:
        True if the condition is met, False otherwise.

    """
    if stat_type == "pitching":
        fields = PITCHING_CORE_STATS
    elif stat_type == "fielding":
        fields = FIELDING_CORE_STATS
    else:
        fields = BATTING_CORE_STATS
    return any(payload.get(field) is not None for field in fields)


def _validate_player_identity(payload: Mapping[str, Any], stat_type: str) -> tuple[bool, str | None]:
    """Validates player identity.

    Args:
        payload: Data payload to process.
        stat_type: Stat Type.

    Returns:
        Tuple result.

    """
    if stat_type == "fielding":
        if normalize_player_id(payload.get("player_id")) is None:
            return False, "invalid_player_id"
        if payload.get("player_name") is not None or payload.get("name") is not None:
            return validate_player_payload(payload)
        return True, None
    return validate_player_payload(payload)


def _validate_season_key(payload: Mapping[str, Any], stat_type: str) -> tuple[bool, str | None]:
    """Validates season key.

    Args:
        payload: Data payload to process.
        stat_type: Stat Type.

    Returns:
        Tuple result.

    """
    try:
        season_key = "year" if stat_type == "fielding" else "season"
        season = int(str(payload.get(season_key)).strip())
    except (TypeError, ValueError):
        return False, "missing_year" if stat_type == "fielding" else "missing_season"
    if season < 1982:
        return False, "missing_year" if stat_type == "fielding" else "missing_season"
    return True, None


def _validate_team_fields(payload: Mapping[str, Any], stat_type: str) -> tuple[bool, str | None]:
    """Validates team fields.

    Args:
        payload: Data payload to process.
        stat_type: Stat Type.

    Returns:
        Tuple result.

    """
    if stat_type == "fielding":
        if not str(payload.get("team_id") or "").strip():
            return False, "missing_team_id"
        if not str(payload.get("position_id") or "").strip():
            return False, "missing_position_id"
    elif not str(payload.get("team_code") or "").strip():
        return False, "missing_team_code"
    return True, None


def _validate_numeric_fields(payload: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Validates numeric fields.

    Args:
        payload: Data payload to process.

    Returns:
        Tuple result.

    """
    for key in NUMERIC_FIELDS:
        if key in payload and not _is_number_like(payload.get(key)):
            return False, "invalid_numeric_stat"
    return True, None


def _validate_batting_consistency(payload: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Validates batting consistency.

    Args:
        payload: Data payload to process.

    Returns:
        Tuple result.

    """
    hits = _number_or_none(payload.get("hits"))
    at_bats = _number_or_none(payload.get("at_bats"))
    plate_appearances = _number_or_none(payload.get("plate_appearances"))
    if hits is not None and at_bats is not None and hits > at_bats:
        return False, "hits_gt_at_bats"
    if at_bats is not None and plate_appearances is not None and at_bats > plate_appearances:
        return False, "at_bats_gt_plate_appearances"
    return True, None


def _validate_pitching_consistency(payload: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Validates pitching consistency.

    Args:
        payload: Data payload to process.

    Returns:
        Tuple result.

    """
    earned_runs = _number_or_none(payload.get("earned_runs"))
    runs_allowed = _number_or_none(payload.get("runs_allowed"))
    if earned_runs is not None and runs_allowed is not None and earned_runs > runs_allowed:
        return False, "earned_runs_gt_runs_allowed"
    return True, None


def validate_season_stat_payload(
    payload: Mapping[str, Any],
    *,
    stat_type: str,
) -> tuple[bool, str | None]:
    """Validates season stat payload.

    Args:
        payload: Data payload to process.

    Returns:
        Tuple result.

    """
    for validator in (
        lambda row: _validate_player_identity(row, stat_type),
        lambda row: _validate_season_key(row, stat_type),
        lambda row: _validate_team_fields(row, stat_type),
        lambda row: (True, None) if _has_core_stats(row, stat_type) else (False, "empty_core_stats"),
        _validate_numeric_fields,
    ):
        ok, reason = validator(payload)
        if not ok:
            return False, reason

    if stat_type == "batting":
        return _validate_batting_consistency(payload)
    if stat_type == "pitching":
        return _validate_pitching_consistency(payload)

    return True, None


def normalize_season_stat_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalizes season stat payload.

    Args:
        payload: Data payload to process.

    Returns:
        Dictionary mapping.

    """
    row = dict(payload)
    row["player_id"] = normalize_player_id(row.get("player_id"))
    if "year" in row:
        row["year"] = int(str(row.get("year")).strip())
    else:
        row["season"] = int(str(row.get("season")).strip())
    if "player_name" in row or "name" in row:
        row["player_name"] = normalize_player_name(row.get("player_name") or row.get("name"))
    return row


def filter_valid_season_stat_payloads(
    payloads: Iterable[Mapping[str, Any]],
    *,
    stat_type: str,
) -> tuple[list[dict[str, Any]], Counter]:
    """Filters valid season stat payloads.

    Args:
        payloads: Payloads.

    Returns:
        Tuple result.

    """
    rows: list[dict[str, Any]] = []
    reasons: Counter = Counter()
    for payload in payloads:
        ok, reason = validate_season_stat_payload(payload, stat_type=stat_type)
        if not ok:
            reasons[reason or "invalid_season_stat"] += 1
            continue
        rows.append(normalize_season_stat_payload(payload))
    return rows, reasons
