"""Validation helpers for player season stat payloads."""
from __future__ import annotations

from collections import Counter
from typing import Any, Iterable, Mapping

from src.utils.player_validation import normalize_player_id, normalize_player_name, validate_player_payload

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
    "intentional_walks",
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


def _is_number_like(value: Any) -> bool:
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
            return True
        except ValueError:
            return False
    return False


def _has_core_stats(payload: Mapping[str, Any], stat_type: str) -> bool:
    if stat_type == "pitching":
        fields = PITCHING_CORE_STATS
    elif stat_type == "fielding":
        fields = FIELDING_CORE_STATS
    else:
        fields = BATTING_CORE_STATS
    return any(payload.get(field) is not None for field in fields)


def validate_season_stat_payload(
    payload: Mapping[str, Any],
    *,
    stat_type: str,
) -> tuple[bool, str | None]:
    if stat_type == "fielding":
        if normalize_player_id(payload.get("player_id")) is None:
            return False, "invalid_player_id"
        if payload.get("player_name") is not None or payload.get("name") is not None:
            ok, reason = validate_player_payload(payload)
            if not ok:
                return False, reason
    else:
        ok, reason = validate_player_payload(payload)
        if not ok:
            return False, reason

    try:
        season_key = "year" if stat_type == "fielding" else "season"
        season = int(str(payload.get(season_key)).strip())
    except (TypeError, ValueError):
        return False, "missing_year" if stat_type == "fielding" else "missing_season"
    if season < 1982:
        return False, "missing_year" if stat_type == "fielding" else "missing_season"

    if stat_type == "fielding":
        if not str(payload.get("team_id") or "").strip():
            return False, "missing_team_id"
        if not str(payload.get("position_id") or "").strip():
            return False, "missing_position_id"
    elif not str(payload.get("team_code") or "").strip():
        return False, "missing_team_code"

    if not _has_core_stats(payload, stat_type):
        return False, "empty_core_stats"

    for key in NUMERIC_FIELDS:
        if key in payload and not _is_number_like(payload.get(key)):
            return False, "invalid_numeric_stat"

    return True, None


def normalize_season_stat_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
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
    rows: list[dict[str, Any]] = []
    reasons: Counter = Counter()
    for payload in payloads:
        ok, reason = validate_season_stat_payload(payload, stat_type=stat_type)
        if not ok:
            reasons[reason or "invalid_season_stat"] += 1
            continue
        rows.append(normalize_season_stat_payload(payload))
    return rows, reasons
