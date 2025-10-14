"""
Parser utilities for retired/inactive player statistics tables.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from src.utils.team_codes import resolve_team_code


def _clean_header(text: str) -> str:
    return (text or "").replace("\n", " ").replace("\r", " ").strip()


def _clean_value(text: Optional[str]) -> str:
    return (text or "").replace(",", "").strip()


def _to_int(value: Optional[str]) -> Optional[int]:
    raw = _clean_value(value)
    if raw in ("", "-", "null"):
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _to_float(value: Optional[str]) -> Optional[float]:
    raw = _clean_value(value)
    if raw in ("", "-", "null"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _innings_to_outs(value: Optional[str]) -> Optional[int]:
    raw = _clean_value(value)
    if raw in ("", "-", "null"):
        return None
    if ":" in raw:
        parts = raw.split(":")
        try:
            innings = int(parts[0])
            remainder = int(parts[1]) if len(parts) > 1 else 0
            return innings * 3 + remainder
        except ValueError:
            return None
    if "." in raw:
        try:
            whole, fraction = raw.split(".")
            outs = int(whole) * 3
            frac_val = int(fraction[0]) if fraction else 0
            return outs + frac_val
        except ValueError:
            return None
    try:
        return int(float(raw) * 3)
    except ValueError:
        return None


def _table_to_dicts(table: Dict[str, Any]) -> Tuple[List[str], List[Dict[str, str]]]:
    headers = [_clean_header(h) for h in (table.get("headers") or [])]
    rows = table.get("rows") or []

    if not headers and rows:
        headers = [_clean_header(h) for h in rows[0]]
        rows = rows[1:]

    dict_rows: List[Dict[str, str]] = []
    for row in rows:
        values = row
        if len(headers) != len(values):
            continue
        dict_rows.append({headers[i]: values[i].strip() for i in range(len(headers))})
    return headers, dict_rows


def _select_tables(tables: List[Dict[str, Any]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    base_rows: List[Dict[str, str]] = []
    adv_rows: List[Dict[str, str]] = []

    for table in tables:
        headers, dict_rows = _table_to_dicts(table)
        header_set = set(headers)
        if {"타수", "안타"} & header_set:
            base_rows.extend(dict_rows)
        elif {"출루율", "장타율", "OPS"} & header_set:
            adv_rows.extend(dict_rows)
    return base_rows, adv_rows


def parse_retired_hitter_tables(
    tables: List[Dict[str, Any]], *, league: str = "REGULAR", level: str = "KBO1"
) -> List[Dict[str, Any]]:
    base_rows, adv_rows = _select_tables(tables)
    advanced_map = {row.get("연도") or row.get("년도"): row for row in adv_rows}
    records: List[Dict[str, Any]] = []

    for row in base_rows:
        season_label = row.get("연도") or row.get("년도")
        if not season_label:
            continue
        if any(marker in season_label for marker in ("통산", "합계", "Career", "Total")):
            continue
        season = _to_int(season_label)
        if season is None:
            continue

        team_name = row.get("팀명") or row.get("팀")
        record: Dict[str, Any] = {
            "season": season,
            "league": league,
            "level": level,
            "team_code": resolve_team_code(team_name),
            "extra_stats": {},
            "source": "PROFILE",
        }

        _apply_stat(record, row, ("경기", "G", "출장", "출장수"), "games", _to_int)
        _apply_stat(record, row, ("타석", "PA"), "plate_appearances", _to_int)
        _apply_stat(record, row, ("타수", "AB"), "at_bats", _to_int)
        _apply_stat(record, row, ("득점", "R"), "runs", _to_int)
        _apply_stat(record, row, ("안타", "H"), "hits", _to_int)
        _apply_stat(record, row, ("2루타", "2B"), "doubles", _to_int)
        _apply_stat(record, row, ("3루타", "3B"), "triples", _to_int)
        _apply_stat(record, row, ("홈런", "HR"), "home_runs", _to_int)
        _apply_stat(record, row, ("타점", "RBI"), "rbi", _to_int)
        _apply_stat(record, row, ("볼넷", "BB"), "walks", _to_int)
        _apply_stat(record, row, ("고의4구", "IBB"), "intentional_walks", _to_int)
        _apply_stat(record, row, ("사구", "HBP"), "hbp", _to_int)
        _apply_stat(record, row, ("삼진", "SO"), "strikeouts", _to_int)
        _apply_stat(record, row, ("도루", "SB"), "stolen_bases", _to_int)
        _apply_stat(record, row, ("도실", "CS"), "caught_stealing", _to_int)
        _apply_stat(record, row, ("희타", "SH"), "sacrifice_hits", _to_int)
        _apply_stat(record, row, ("희비", "SF"), "sacrifice_flies", _to_int)
        _apply_stat(record, row, ("병살", "GDP"), "gdp", _to_int)
        _apply_stat(record, row, ("타율", "AVG"), "avg", _to_float)

        adv = advanced_map.get(season_label)
        if adv:
            _apply_stat(record, adv, ("출루율", "OBP"), "obp", _to_float)
            _apply_stat(record, adv, ("장타율", "SLG"), "slg", _to_float)
            _apply_stat(record, adv, ("OPS",), "ops", _to_float)
            _apply_stat(record, adv, ("ISO",), "iso", _to_float)
            _apply_stat(record, adv, ("BABIP",), "babip", _to_float)
            _merge_extra_stats(record, adv, consumed=record.get("_consumed_keys", set()))

        _merge_extra_stats(record, row, consumed=record.get("_consumed_keys", set()))
        _cleanup_consumed(record)
        records.append(record)

    return records


def parse_retired_pitcher_table(
    table: Dict[str, Any], *, league: str = "REGULAR", level: str = "KBO1"
) -> List[Dict[str, Any]]:
    _, rows = _table_to_dicts(table)
    records: List[Dict[str, Any]] = []

    for row in rows:
        season_label = row.get("연도") or row.get("년도")
        if not season_label:
            continue
        if any(marker in season_label for marker in ("통산", "합계", "Career", "Total")):
            continue
        season = _to_int(season_label)
        if season is None:
            continue

        team_name = row.get("팀명") or row.get("팀")
        record: Dict[str, Any] = {
            "season": season,
            "league": league,
            "level": level,
            "team_code": resolve_team_code(team_name),
            "extra_stats": {},
            "source": "PROFILE",
        }

        _apply_stat(record, row, ("경기", "G"), "games", _to_int)
        _apply_stat(record, row, ("선발", "GS"), "games_started", _to_int)
        _apply_stat(record, row, ("승", "W"), "wins", _to_int)
        _apply_stat(record, row, ("패", "L"), "losses", _to_int)
        _apply_stat(record, row, ("세", "세이브", "SV"), "saves", _to_int)
        _apply_stat(record, row, ("홀드", "HLD"), "holds", _to_int)
        _apply_stat(record, row, ("이닝", "IP"), "innings_outs", _innings_to_outs)
        _apply_stat(record, row, ("피안타", "H"), "hits_allowed", _to_int)
        _apply_stat(record, row, ("실점", "R"), "runs_allowed", _to_int)
        _apply_stat(record, row, ("자책", "ER"), "earned_runs", _to_int)
        _apply_stat(record, row, ("피홈런", "HR"), "home_runs_allowed", _to_int)
        _apply_stat(record, row, ("볼넷", "BB"), "walks_allowed", _to_int)
        _apply_stat(record, row, ("고의4구", "IBB"), "intentional_walks", _to_int)
        _apply_stat(record, row, ("사구", "HBP"), "hit_batters", _to_int)
        _apply_stat(record, row, ("삼진", "SO"), "strikeouts", _to_int)
        _apply_stat(record, row, ("폭투", "WP"), "wild_pitches", _to_int)
        _apply_stat(record, row, ("보크", "BK"), "balks", _to_int)
        _apply_stat(record, row, ("평균자책", "ERA"), "era", _to_float)
        _apply_stat(record, row, ("WHIP",), "whip", _to_float)
        _apply_stat(record, row, ("FIP",), "fip", _to_float)
        _apply_stat(record, row, ("K/9",), "k_per_nine", _to_float)
        _apply_stat(record, row, ("BB/9",), "bb_per_nine", _to_float)
        _apply_stat(record, row, ("K/BB",), "kbb", _to_float)

        _merge_extra_stats(record, row, consumed=record.get("_consumed_keys", set()))
        _cleanup_consumed(record)
        records.append(record)

    return records


def _apply_stat(
    record: Dict[str, Any],
    row: Dict[str, str],
    keys: Tuple[str, ...],
    field: str,
    converter,
) -> None:
    consumed = record.setdefault("_consumed_keys", set())
    for key in keys:
        if key in row and row[key]:
            record[field] = converter(row[key])
            consumed.add(key)
            break


def _merge_extra_stats(record: Dict[str, Any], row: Dict[str, str], consumed: set) -> None:
    extra = record.setdefault("extra_stats", {})
    for key, value in row.items():
        if key in consumed:
            continue
        clean_key = _clean_header(key)
        if not clean_key:
            continue
        extra[clean_key] = value.strip()


def _cleanup_consumed(record: Dict[str, Any]) -> None:
    record.pop("_consumed_keys", None)
    if record.get("extra_stats") == {}:
        record["extra_stats"] = None


__all__ = [
    "parse_retired_hitter_tables",
    "parse_retired_pitcher_table",
]
