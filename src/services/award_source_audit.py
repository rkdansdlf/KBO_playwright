"""Pure audit calculations for the KBO awards ingestion path."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.constants import KST

AWARD_SOURCE_KEYS = ("kbo_awards_wikipedia", "kbo_awards_yagoonara")
MIN_AWARD_YEAR = 1982


@dataclass(frozen=True)
class AwardAuditInput:
    """Database and optional probe inputs for an awards audit."""

    award_rows: list[object]
    data_sources: list[object]
    snapshots: list[object]
    player_ids: set[int]
    team_codes: set[str]
    probe_runs: list[object]
    current_year: int | None = None


def build_award_audit(
    audit_input: AwardAuditInput,
) -> dict[str, Any]:
    """Build a source-to-storage audit for awards without mutating data.

    Args:
        audit_input: Database rows and optional live probe summaries.

    Returns:
        JSON-serializable audit payload.

    """
    quality = audit_award_rows(
        audit_input.award_rows,
        player_ids=audit_input.player_ids,
        team_codes=audit_input.team_codes,
        current_year=audit_input.current_year or datetime.now(KST).year,
    )
    source_details = _source_details(audit_input.data_sources, audit_input.snapshots, audit_input.probe_runs)
    stored_records = len(audit_input.award_rows)
    parsed_records = _parsed_record_total(audit_input.probe_runs, audit_input.snapshots)
    status = _overall_status(stored_records, quality, source_details, parsed_records)
    return {
        "source": "awards",
        "status": status,
        "raw_records": sum(source["raw_snapshot_rows"] for source in source_details),
        "parsed_records": parsed_records,
        "stored_records": stored_records,
        "quarantined_records": 0,
        "quarantine_status": "not_configured",
        "source_details": source_details,
        "quality": quality,
        "ok": status == "healthy",
        "probe_enabled": bool(audit_input.probe_runs),
    }


def audit_award_rows(
    rows: list[object],
    *,
    player_ids: set[int],
    team_codes: set[str],
    current_year: int,
) -> dict[str, Any]:
    """Check award row completeness, natural-key uniqueness, and links."""
    natural_keys: Counter[tuple[object, ...]] = Counter()
    award_types: set[str] = set()
    missing_required_fields = 0
    invalid_season_rows = 0
    invalid_player_ids = 0
    invalid_team_codes = 0
    missing_player_links = 0
    missing_team_links = 0
    years: list[int] = []

    for row in rows:
        values = _audit_award_row(row, player_ids, team_codes, current_year)
        missing_required_fields += values["missing_required"]
        invalid_season_rows += values["invalid_season"]
        invalid_player_ids += values["invalid_player"]
        invalid_team_codes += values["invalid_team"]
        missing_player_links += values["missing_player_link"]
        missing_team_links += values["missing_team_link"]
        if values["year"] is not None:
            years.append(values["year"])
        if values["award_type"]:
            award_types.add(values["award_type"])
        if values["natural_key"] is not None:
            natural_keys[values["natural_key"]] += 1

    duplicate_natural_keys = sum(count - 1 for count in natural_keys.values() if count > 1)
    defects = {
        "duplicate_natural_keys": duplicate_natural_keys,
        "invalid_season_rows": invalid_season_rows,
        "missing_required_fields": missing_required_fields,
        "invalid_player_ids": invalid_player_ids,
        "invalid_team_codes": invalid_team_codes,
    }
    return {
        "stored_records": len(rows),
        "min_year": min(years) if years else None,
        "max_year": max(years) if years else None,
        "award_types": sorted(award_types),
        **defects,
        "missing_player_links": missing_player_links,
        "missing_team_links": missing_team_links,
        "quality_ok": not any(defects.values()),
    }


def _source_details(
    data_sources: list[object],
    snapshots: list[object],
    probe_runs: list[object],
) -> list[dict[str, Any]]:
    """Combine source registry, snapshot, and optional live probe evidence."""
    registered = {_value(row, "source_key"): row for row in data_sources}
    source_ids = {_value(row, "id"): _value(row, "source_key") for row in data_sources}
    snapshots_by_source: dict[str, list[object]] = defaultdict(list)
    for snapshot in snapshots:
        source_key = source_ids.get(_value(snapshot, "data_source_id"))
        if source_key:
            snapshots_by_source[source_key].append(snapshot)
    probes_by_source: dict[str, list[object]] = defaultdict(list)
    for run in probe_runs:
        source_key = _value(run, "source_key")
        if source_key:
            probes_by_source[source_key].append(run)

    details = []
    for source_key in AWARD_SOURCE_KEYS:
        source = registered.get(source_key)
        source_snapshots = snapshots_by_source[source_key]
        probe_rows = probes_by_source[source_key]
        parse_status = Counter(_text(_value(row, "parse_status")) or "unknown" for row in source_snapshots)
        probe_errors = [_text(_value(row, "error")) for row in probe_rows if _value(row, "error")]
        probe_parsed_count = sum(int(_value(row, "parsed_records") or 0) for row in probe_rows) if probe_rows else None
        snapshot_parsed_count = _snapshot_parsed_record_total(source_snapshots)
        latest_snapshot = max(
            (_value(row, "fetched_at") for row in source_snapshots if _value(row, "fetched_at") is not None),
            default=None,
        )
        details.append(
            {
                "source_key": source_key,
                "registered": source is not None,
                "active": bool(_value(source, "is_active")) if source is not None else False,
                "raw_snapshot_rows": len(source_snapshots),
                "parse_status_counts": dict(sorted(parse_status.items())),
                "latest_snapshot_at": _serialize_datetime(latest_snapshot),
                "last_success_at": _serialize_datetime(_value(source, "last_success_at"))
                if source is not None
                else None,
                "probe_fetched": any(bool(_value(row, "fetched")) for row in probe_rows) if probe_rows else None,
                "parsed_records": probe_parsed_count if probe_parsed_count is not None else snapshot_parsed_count,
                "probe_errors": probe_errors,
            },
        )
    return details


def _overall_status(
    stored_records: int,
    quality: dict[str, Any],
    source_details: list[dict[str, Any]],
    parsed_records: int | None,
) -> str:
    """Classify the first failing stage in the awards pipeline."""
    status = "healthy"
    if not quality["quality_ok"]:
        status = "quality_failed"
    elif any(source["probe_errors"] for source in source_details):
        status = "probe_failed"
    raw_records = sum(source["raw_snapshot_rows"] for source in source_details)
    if status == "healthy":
        if parsed_records and stored_records == 0:
            status = "persistence_missing"
        elif stored_records == 0 and raw_records == 0:
            status = "missing_upstream"
        elif stored_records == 0 and raw_records > 0:
            status = "parser_or_persistence_missing"
        elif stored_records > 0 and raw_records == 0:
            status = "stored_without_snapshot"
    return status


def _audit_award_row(
    row: object,
    player_ids: set[int],
    team_codes: set[str],
    current_year: int,
) -> dict[str, Any]:
    """Return normalized quality findings for one stored award row."""
    year = _int_value(_value(row, "year"))
    award_type = _text(_value(row, "award_type"))
    category = _text(_value(row, "category")) or None
    player_name = _text(_value(row, "player_name"))
    team_name = _text(_value(row, "team_name"))
    missing_required = int(year is None or not award_type or not player_name or not team_name)
    invalid_season = int(year is not None and (year < MIN_AWARD_YEAR or year > current_year))
    player_id = _value(row, "player_id")
    team_code = _text(_value(row, "team_code"))
    return {
        "year": year,
        "award_type": award_type,
        "missing_required": missing_required,
        "invalid_season": invalid_season,
        "invalid_player": int(player_id is not None and player_id not in player_ids),
        "invalid_team": int(bool(team_code) and team_code not in team_codes),
        "missing_player_link": int(player_id is None),
        "missing_team_link": int(not team_code),
        "natural_key": (year, award_type, category, player_name, team_name) if missing_required == 0 else None,
    }


def _parsed_record_total(probe_runs: list[object] | None, snapshots: list[object]) -> int | None:
    """Return live-probe counts or persisted snapshot parser counts."""
    if probe_runs:
        return sum(int(_value(run, "parsed_records") or 0) for run in probe_runs)
    return _snapshot_parsed_record_total(snapshots)


def _snapshot_parsed_record_total(snapshots: list[object]) -> int | None:
    """Sum parser counts persisted in raw snapshot capture metadata."""
    parsed_counts: list[int] = []
    for snapshot in snapshots:
        metadata = _value(snapshot, "capture_metadata")
        if not isinstance(metadata, Mapping) or metadata.get("parsed_records") is None:
            continue
        parsed_counts.append(int(metadata["parsed_records"]))
    return sum(parsed_counts) if parsed_counts else None


def _value(row: object | None, key: str) -> object:
    """Read a field from an ORM object or mapping-like test row."""
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _text(value: object) -> str:
    """Normalize an optional audit value to trimmed text."""
    return str(value).strip() if value is not None else ""


def _int_value(value: object) -> int | None:
    """Normalize an integer-like database value."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _serialize_datetime(value: object) -> str | None:
    """Serialize datetime-like values for JSON output."""
    return value.isoformat() if isinstance(value, datetime) else (_text(value) or None)
