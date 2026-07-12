"""Parser utilities for retired/inactive player statistics tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.constants import (
    KBO_FOUNDING_YEAR,
    KBO_MAX_GAMES_PER_SEASON,
    KBO_MAX_HOME_RUNS_PER_SEASON,
    KBO_MAX_VALID_SEASON,
    KBO_MAX_WINS_PER_SEASON,
)
from src.utils.team_codes import resolve_kbo_legacy_team_code
from src.utils.type_helpers import parse_innings_to_outs, safe_float_or_none, safe_int_or_none

if TYPE_CHECKING:
    from collections.abc import Callable


def _clean_header(text: str) -> str:
    return (text or "").replace("\n", " ").replace("\r", " ").strip()


def _table_to_dicts(table: dict[str, Any]) -> tuple[list[str], list[dict[str, str]]]:
    headers = [_clean_header(h) for h in (table.get("headers") or [])]
    rows = table.get("rows") or []

    if not headers and rows:
        headers = [_clean_header(h) for h in rows[0]]
        rows = rows[1:]

    dict_rows: list[dict[str, str]] = []
    for row in rows:
        values = row
        if len(headers) != len(values):
            continue
        dict_rows.append({headers[i]: values[i].strip() for i in range(len(headers))})
    return headers, dict_rows


def _select_tables(tables: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    base_rows: list[dict[str, str]] = []
    adv_rows: list[dict[str, str]] = []

    for table in tables:
        headers, dict_rows = _table_to_dicts(table)
        header_set = set(headers)

        # Priority: Check for explicit table type marker (for Futures crawler)
        if table.get("_table_type") == "HITTER":
            base_rows.extend(dict_rows)
            continue

        # Fallback: keyword matching (support both Korean and English abbreviations)
        is_base = bool({"타수", "안타", "AB", "H"} & header_set)
        is_adv = bool({"출루율", "장타율", "OPS", "OBP", "SLG"} & header_set)

        if is_base:
            base_rows.extend(dict_rows)
            if is_adv:
                adv_rows.extend(dict_rows)
        elif is_adv:
            adv_rows.extend(dict_rows)

    return base_rows, adv_rows


def parse_retired_hitter_tables(
    tables: list[dict[str, Any]],
    *,
    league: str = "REGULAR",
    level: str = "KBO1",
) -> list[dict[str, Any]]:
    """Parse retired hitter tables.

    Args:
        tables: Tables.
        league: League.
        level: Level.
        tables: Tables.
        league: League.
        level: Level.
        tables: Tables.

    Returns:
        List of results.

    """
    base_rows, adv_rows = _select_tables(tables)

    advanced_map = {row.get("연도") or row.get("년도"): row for row in adv_rows}
    records: list[dict[str, Any]] = []

    for row in base_rows:
        season_label = (row.get("연도") or row.get("년도") or "").strip()
        if not season_label:
            continue

        # Strict exclusion of summary rows
        if any(marker in season_label for marker in ("통산", "합계", "Career", "Total", "연도")):
            continue

        season = safe_int_or_none(season_label)
        if season is None or season < KBO_FOUNDING_YEAR or season > KBO_MAX_VALID_SEASON:
            continue

        team_name = row.get("팀명") or row.get("팀")
        record: dict[str, Any] = {
            "season": season,
            "league": league,
            "level": level,
            "team_code": resolve_kbo_legacy_team_code(team_name, season_year=season),
            "extra_stats": {},
            "source": "PROFILE",
        }

        _apply_stat(record, row, ("경기", "G", "출장", "출장수"), "games", safe_int_or_none)

        # Safety Guard: If a single season has > 165 games, it's likely a summary row we missed
        if record.get("games", 0) > KBO_MAX_GAMES_PER_SEASON:
            continue

        _apply_stat(record, row, ("타석", "PA"), "plate_appearances", safe_int_or_none)
        _apply_stat(record, row, ("타수", "AB"), "at_bats", safe_int_or_none)
        _apply_stat(record, row, ("득점", "R"), "runs", safe_int_or_none)
        _apply_stat(record, row, ("안타", "H"), "hits", safe_int_or_none)
        _apply_stat(record, row, ("2루타", "2B"), "doubles", safe_int_or_none)
        _apply_stat(record, row, ("3루타", "3B"), "triples", safe_int_or_none)
        _apply_stat(record, row, ("홈런", "HR"), "home_runs", safe_int_or_none)

        # Another Guard: KBO single season HR record is 56 (Lee Seung-yeop)
        if record.get("home_runs", 0) > KBO_MAX_HOME_RUNS_PER_SEASON:
            continue

        _apply_stat(record, row, ("타점", "RBI"), "rbi", safe_int_or_none)
        _apply_stat(record, row, ("볼넷", "BB"), "walks", safe_int_or_none)
        _apply_stat(record, row, ("고의4구", "IBB"), "intentional_walks", safe_int_or_none)
        _apply_stat(record, row, ("사구", "HBP"), "hbp", safe_int_or_none)
        _apply_stat(record, row, ("삼진", "SO"), "strikeouts", safe_int_or_none)
        _apply_stat(record, row, ("도루", "SB"), "stolen_bases", safe_int_or_none)
        _apply_stat(record, row, ("도실", "CS"), "caught_stealing", safe_int_or_none)
        _apply_stat(record, row, ("희타", "SH"), "sacrifice_hits", safe_int_or_none)
        _apply_stat(record, row, ("희비", "SF"), "sacrifice_flies", safe_int_or_none)
        _apply_stat(record, row, ("병살", "GDP"), "gdp", safe_int_or_none)
        _apply_stat(record, row, ("타율", "AVG"), "avg", safe_float_or_none)

        adv = advanced_map.get(season_label)
        if adv:
            _apply_stat(record, adv, ("출루율", "OBP"), "obp", safe_float_or_none)
            _apply_stat(record, adv, ("장타율", "SLG"), "slg", safe_float_or_none)
            _apply_stat(record, adv, ("OPS",), "ops", safe_float_or_none)
            _apply_stat(record, adv, ("ISO",), "iso", safe_float_or_none)
            _apply_stat(record, adv, ("BABIP",), "babip", safe_float_or_none)
            _merge_extra_stats(record, adv, consumed=record.get("_consumed_keys", set()))

        _merge_extra_stats(record, row, consumed=record.get("_consumed_keys", set()))
        _cleanup_consumed(record)
        records.append(record)

    return records


def parse_retired_pitcher_table(
    table: dict[str, Any],
    *,
    league: str = "REGULAR",
    level: str = "KBO1",
) -> list[dict[str, Any]]:
    """Parse retired pitcher table.

    Args:
        table: Table.
        league: League.
        level: Level.
        table: Table.
        league: League.
        level: Level.
        table: Table.

    Returns:
        List of results.

    """
    _, rows = _table_to_dicts(table)

    records: list[dict[str, Any]] = []

    for row in rows:
        season_label = (row.get("연도") or row.get("년도") or "").strip()
        if not season_label:
            continue

        # Strict exclusion
        if any(marker in season_label for marker in ("통산", "합계", "Career", "Total", "연도")):
            continue

        season = safe_int_or_none(season_label)
        if season is None or season < KBO_FOUNDING_YEAR or season > KBO_MAX_VALID_SEASON:
            continue

        team_name = row.get("팀명") or row.get("팀")
        record: dict[str, Any] = {
            "season": season,
            "league": league,
            "level": level,
            "team_code": resolve_kbo_legacy_team_code(team_name, season_year=season),
            "extra_stats": {},
            "source": "PROFILE",
        }

        _apply_stat(record, row, ("경기", "G"), "games", safe_int_or_none)

        # Guard
        if record.get("games", 0) > KBO_MAX_GAMES_PER_SEASON:
            continue

        _apply_stat(record, row, ("선발", "GS"), "games_started", safe_int_or_none)
        _apply_stat(record, row, ("승", "W"), "wins", safe_int_or_none)

        # Guard: KBO single season win record is 30 (Jang Myeong-bu)
        if record.get("wins", 0) > KBO_MAX_WINS_PER_SEASON:
            continue

        _apply_stat(record, row, ("패", "L"), "losses", safe_int_or_none)
        _apply_stat(record, row, ("세", "세이브", "SV"), "saves", safe_int_or_none)
        _apply_stat(record, row, ("홀드", "HLD"), "holds", safe_int_or_none)
        _apply_stat(record, row, ("이닝", "IP"), "innings_outs", parse_innings_to_outs)
        _apply_stat(record, row, ("피안타", "H"), "hits_allowed", safe_int_or_none)
        _apply_stat(record, row, ("실점", "R"), "runs_allowed", safe_int_or_none)
        _apply_stat(record, row, ("자책", "ER"), "earned_runs", safe_int_or_none)
        _apply_stat(record, row, ("피홈런", "HR"), "home_runs_allowed", safe_int_or_none)
        _apply_stat(record, row, ("볼넷", "BB"), "walks_allowed", safe_int_or_none)
        _apply_stat(record, row, ("고의4구", "IBB"), "intentional_walks", safe_int_or_none)
        _apply_stat(record, row, ("사구", "HBP"), "hit_batters", safe_int_or_none)
        _apply_stat(record, row, ("삼진", "SO"), "strikeouts", safe_int_or_none)
        _apply_stat(record, row, ("폭투", "WP"), "wild_pitches", safe_int_or_none)
        _apply_stat(record, row, ("보크", "BK"), "balks", safe_int_or_none)
        _apply_stat(record, row, ("평균자책", "ERA"), "era", safe_float_or_none)
        _apply_stat(record, row, ("WHIP",), "whip", safe_float_or_none)
        _apply_stat(record, row, ("FIP",), "fip", safe_float_or_none)
        _apply_stat(record, row, ("K/9",), "k_per_nine", safe_float_or_none)
        _apply_stat(record, row, ("BB/9",), "bb_per_nine", safe_float_or_none)
        _apply_stat(record, row, ("K/BB",), "kbb", safe_float_or_none)

        _merge_extra_stats(record, row, consumed=record.get("_consumed_keys", set()))
        _cleanup_consumed(record)
        records.append(record)

    return records


def _apply_stat(
    record: dict[str, Any],
    row: dict[str, str],
    keys: tuple[str, ...],
    field: str,
    converter: Callable[[str], int | float | None],
) -> None:
    consumed = record.setdefault("_consumed_keys", set())
    for key in keys:
        if row.get(key):
            record[field] = converter(row[key])
            consumed.add(key)
            break


def _merge_extra_stats(record: dict[str, Any], row: dict[str, str], consumed: set[str]) -> None:
    extra = record.setdefault("extra_stats", {})
    for key, value in row.items():
        if key in consumed:
            continue
        clean_key = _clean_header(key)
        if not clean_key:
            continue
        extra[clean_key] = value.strip()


def _cleanup_consumed(record: dict[str, Any]) -> None:
    record.pop("_consumed_keys", None)
    if record.get("extra_stats") == {}:
        record["extra_stats"] = None


__all__ = [
    "parse_retired_hitter_tables",
    "parse_retired_pitcher_table",
]
