#!/usr/bin/env python3
"""Repair historical team/franchise metadata from the canonical history map."""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.models.franchise import Franchise
from src.models.team import Team, TeamCodeMap
from src.models.team_history import TeamHistory
from src.utils.team_codes import team_code_from_game_id_segment
from src.utils.team_history import FRANCHISE_CANONICAL_CODE, find_team_history_entry, iter_team_history

DEFAULT_DB_URL = "sqlite:///./data/kbo_dev.db"
SOURCE_URL = "https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx"

FRANCHISE_ROWS: dict[int, dict[str, Any]] = {
    1: {"name": "삼성 라이온즈", "original_code": "SS", "current_code": "SS"},
    2: {"name": "롯데 자이언츠", "original_code": "LT", "current_code": "LT"},
    3: {"name": "LG 트윈스", "original_code": "MBC", "current_code": "LG"},
    4: {"name": "두산 베어스", "original_code": "OB", "current_code": "DB"},
    5: {"name": "KIA 타이거즈", "original_code": "HT", "current_code": "KIA"},
    6: {"name": "현대 유니콘스 계열", "original_code": "SM", "current_code": "HU"},
    7: {"name": "한화 이글스", "original_code": "BE", "current_code": "HH"},
    8: {"name": "SSG 랜더스", "original_code": "SK", "current_code": "SSG"},
    9: {"name": "NC 다이노스", "original_code": "NC", "current_code": "NC"},
    10: {"name": "kt wiz", "original_code": "KT", "current_code": "KT"},
    11: {"name": "키움 히어로즈", "original_code": "WO", "current_code": "KH"},
    12: {"name": "쌍방울 레이더스", "original_code": "SL", "current_code": "SL"},
}

TEAM_ROWS: dict[str, dict[str, Any]] = {
    "SS": {
        "team_name": "삼성 라이온즈",
        "team_short_name": "삼성",
        "city": "대구",
        "founded_year": 1982,
        "stadium_name": "대구 삼성 라이온즈 파크",
        "franchise_id": 1,
        "is_active": True,
        "aliases": [],
    },
    "LT": {
        "team_name": "롯데 자이언츠",
        "team_short_name": "롯데",
        "city": "부산",
        "founded_year": 1982,
        "stadium_name": "부산 사직 야구장",
        "franchise_id": 2,
        "is_active": True,
        "aliases": [],
    },
    "MBC": {
        "team_name": "MBC 청룡",
        "team_short_name": "MBC",
        "city": "서울",
        "founded_year": 1982,
        "stadium_name": "잠실야구장",
        "franchise_id": 3,
        "is_active": False,
        "aliases": [],
    },
    "LG": {
        "team_name": "LG 트윈스",
        "team_short_name": "LG",
        "city": "서울",
        "founded_year": 1990,
        "stadium_name": "잠실야구장",
        "franchise_id": 3,
        "is_active": True,
        "aliases": [],
    },
    "OB": {
        "team_name": "OB 베어스",
        "team_short_name": "OB",
        "city": "서울",
        "founded_year": 1982,
        "stadium_name": "잠실야구장",
        "franchise_id": 4,
        "is_active": False,
        "aliases": [],
    },
    "DB": {
        "team_name": "두산 베어스",
        "team_short_name": "두산",
        "city": "서울",
        "founded_year": 1999,
        "stadium_name": "잠실야구장",
        "franchise_id": 4,
        "is_active": True,
        "aliases": ["DO"],
    },
    "HT": {
        "team_name": "해태 타이거즈",
        "team_short_name": "해태",
        "city": "광주",
        "founded_year": 1982,
        "stadium_name": "광주 무등경기장 야구장",
        "franchise_id": 5,
        "is_active": False,
        "aliases": [],
    },
    "KIA": {
        "team_name": "KIA 타이거즈",
        "team_short_name": "KIA",
        "city": "광주",
        "founded_year": 2001,
        "stadium_name": "광주-기아 챔피언스 필드",
        "franchise_id": 5,
        "is_active": True,
        "aliases": [],
    },
    "SM": {
        "team_name": "삼미 슈퍼스타즈",
        "team_short_name": "삼미",
        "city": "인천",
        "founded_year": 1982,
        "stadium_name": "인천공설운동장 야구장",
        "franchise_id": 6,
        "is_active": False,
        "aliases": [],
    },
    "CB": {
        "team_name": "청보 핀토스",
        "team_short_name": "청보",
        "city": "인천",
        "founded_year": 1985,
        "stadium_name": "인천공설운동장 야구장",
        "franchise_id": 6,
        "is_active": False,
        "aliases": [],
    },
    "TP": {
        "team_name": "태평양 돌핀스",
        "team_short_name": "태평양",
        "city": "인천",
        "founded_year": 1988,
        "stadium_name": "인천공설운동장 야구장",
        "franchise_id": 6,
        "is_active": False,
        "aliases": [],
    },
    "HU": {
        "team_name": "현대 유니콘스",
        "team_short_name": "현대",
        "city": "수원",
        "founded_year": 1996,
        "stadium_name": "수원야구장",
        "franchise_id": 6,
        "is_active": False,
        "aliases": ["HD"],
    },
    "BE": {
        "team_name": "빙그레 이글스",
        "team_short_name": "빙그레",
        "city": "대전",
        "founded_year": 1986,
        "stadium_name": "대전한밭야구장",
        "franchise_id": 7,
        "is_active": False,
        "aliases": [],
    },
    "HH": {
        "team_name": "한화 이글스",
        "team_short_name": "한화",
        "city": "대전",
        "founded_year": 1994,
        "stadium_name": "대전 한화생명 이글스 파크",
        "franchise_id": 7,
        "is_active": True,
        "aliases": [],
    },
    "SK": {
        "team_name": "SK 와이번스",
        "team_short_name": "SK",
        "city": "인천",
        "founded_year": 2000,
        "stadium_name": "인천문학야구장",
        "franchise_id": 8,
        "is_active": False,
        "aliases": [],
    },
    "SSG": {
        "team_name": "SSG 랜더스",
        "team_short_name": "SSG",
        "city": "인천",
        "founded_year": 2021,
        "stadium_name": "인천SSG랜더스필드",
        "franchise_id": 8,
        "is_active": True,
        "aliases": [],
    },
    "NC": {
        "team_name": "NC 다이노스",
        "team_short_name": "NC",
        "city": "창원",
        "founded_year": 2011,
        "stadium_name": "창원NC파크",
        "franchise_id": 9,
        "is_active": True,
        "aliases": [],
    },
    "KT": {
        "team_name": "kt wiz",
        "team_short_name": "kt",
        "city": "수원",
        "founded_year": 2013,
        "stadium_name": "수원 kt wiz 파크",
        "franchise_id": 10,
        "is_active": True,
        "aliases": [],
    },
    "WO": {
        "team_name": "우리 히어로즈",
        "team_short_name": "우리",
        "city": "서울",
        "founded_year": 2008,
        "stadium_name": "목동야구장",
        "franchise_id": 11,
        "is_active": False,
        "aliases": [],
    },
    "NX": {
        "team_name": "넥센 히어로즈",
        "team_short_name": "넥센",
        "city": "서울",
        "founded_year": 2010,
        "stadium_name": "고척스카이돔",
        "franchise_id": 11,
        "is_active": False,
        "aliases": [],
    },
    "KH": {
        "team_name": "키움 히어로즈",
        "team_short_name": "키움",
        "city": "서울",
        "founded_year": 2019,
        "stadium_name": "고척스카이돔",
        "franchise_id": 11,
        "is_active": True,
        "aliases": ["KI"],
    },
    "SL": {
        "team_name": "쌍방울 레이더스",
        "team_short_name": "쌍방울",
        "city": "전주",
        "founded_year": 1990,
        "stadium_name": "전주야구장",
        "franchise_id": 12,
        "is_active": False,
        "aliases": [],
    },
}

OPTIONAL_ALIAS_ROWS: dict[str, dict[str, Any]] = {
    "DO": {
        "team_name": "두산 베어스",
        "team_short_name": "두산",
        "city": "서울",
        "founded_year": 1999,
        "stadium_name": "잠실야구장",
        "franchise_id": 4,
        "is_active": False,
        "aliases": ["DB"],
    },
    "KI": {
        "team_name": "키움 히어로즈",
        "team_short_name": "키움",
        "city": "서울",
        "founded_year": 2019,
        "stadium_name": "고척스카이돔",
        "franchise_id": 11,
        "is_active": False,
        "aliases": ["KH"],
    },
}

DIRECT_FACT_TABLES = (
    ("player_season_batting", "season", "team_code"),
    ("player_season_pitching", "season", "team_code"),
    ("player_season_fielding", "year", "team_id"),
    ("player_season_baserunning", "year", "team_id"),
    ("team_season_batting", "season", "team_id"),
    ("team_season_pitching", "season", "team_id"),
)
DATE_FACT_TABLES = (("team_daily_roster", "roster_date", "team_code"),)
GAME_ID_FACT_TABLES = (
    ("game_inning_scores", "game_id", "team_code"),
    ("game_lineups", "game_id", "team_code"),
    ("game_batting_stats", "game_id", "team_code"),
    ("game_pitching_stats", "game_id", "team_code"),
)
GAME_TEAM_COLUMNS = (
    ("away_team", "away_franchise_id"),
    ("home_team", "home_franchise_id"),
    ("winning_team", "winning_franchise_id"),
)

POSTGRES_RESOLVE_TEAM_CODE_SQL = """
CREATE OR REPLACE FUNCTION security.resolve_team_code_for_season(input_code text, season_year integer)
RETURNS text
LANGUAGE plpgsql
STABLE
AS $function$
DECLARE
    raw text;
    franchise integer;
    resolved text;
BEGIN
    IF input_code IS NULL THEN
        RETURN NULL;
    END IF;

    raw := UPPER(BTRIM(input_code));
    IF raw = '' THEN
        RETURN raw;
    END IF;

    IF raw = 'LOT' THEN
        raw := 'LT';
    ELSIF raw = 'KW' THEN
        raw := 'KH';
    END IF;

    IF season_year IS NULL THEN
        RETURN raw;
    END IF;

    IF raw = 'SSG' AND season_year BETWEEN 1991 AND 1999 THEN
        raw := 'SL';
    END IF;

    SELECT th.team_code
    INTO resolved
    FROM public.team_history th
    WHERE th.season = season_year
      AND UPPER(th.team_code) = raw
    LIMIT 1;

    IF resolved IS NOT NULL THEN
        RETURN resolved;
    END IF;

    SELECT t.franchise_id
    INTO franchise
    FROM public.teams t
    WHERE UPPER(t.team_id) = raw
       OR COALESCE(t.aliases, '') ILIKE ('%' || raw || '%')
    ORDER BY CASE WHEN UPPER(t.team_id) = raw THEN 0 ELSE 1 END
    LIMIT 1;

    IF franchise IS NULL THEN
        RETURN raw;
    END IF;

    SELECT th.team_code
    INTO resolved
    FROM public.team_history th
    WHERE th.season = season_year
      AND th.franchise_id = franchise
    LIMIT 1;

    RETURN COALESCE(resolved, raw);
END;
$function$;
"""

POSTGRES_NORMALIZE_TEAM_CODE_COLUMN_SQL = """
CREATE OR REPLACE FUNCTION security.normalize_team_code_column()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    row_payload jsonb;
    season_year integer;
    raw_year text;
BEGIN
    IF NEW.team_code IS NULL THEN
        RETURN NEW;
    END IF;

    row_payload := to_jsonb(NEW);
    IF row_payload ? 'season' THEN
        raw_year := row_payload ->> 'season';
    ELSIF row_payload ? 'year' THEN
        raw_year := row_payload ->> 'year';
    ELSIF row_payload ? 'roster_date' THEN
        raw_year := SUBSTR(row_payload ->> 'roster_date', 1, 4);
    ELSIF row_payload ? 'game_id' THEN
        raw_year := SUBSTR(row_payload ->> 'game_id', 1, 4);
    END IF;

    IF raw_year ~ '^[0-9]{4}$' THEN
        season_year := raw_year::integer;
    END IF;

    NEW.team_code = security.resolve_team_code_for_season(NEW.team_code, season_year);
    RETURN NEW;
END;
$function$;
"""


def _merge_aliases(existing: Any, desired: list[str]) -> list[str]:
    values: list[str] = []
    if isinstance(existing, list):
        existing_values = [str(item) for item in existing if item]
        if existing_values and all(len(value) == 1 for value in existing_values) and {"{", "}"} & set(existing_values):
            values.extend(re.findall(r"[A-Za-z0-9]{2,10}", "".join(existing_values)))
        else:
            values.extend(existing_values)
    elif isinstance(existing, str) and existing:
        values.extend(re.findall(r"[A-Za-z0-9]{2,10}", existing))
    values.extend(desired)
    return sorted({value.upper() for value in values if value and len(value.strip()) >= 2})


def _source_metadata(franchise_id: int, status: str) -> dict[str, Any]:
    return {
        "source": SOURCE_URL,
        "history_model": "historical_franchise_split",
        "status": status,
        "franchise_id": franchise_id,
    }


def _upsert_franchises(session) -> int:
    changed = 0
    dissolved_ids = {6, 12}
    missing_rows: list[tuple[int, dict[str, Any], str]] = []
    for franchise_id, row in FRANCHISE_ROWS.items():
        franchise = session.get(Franchise, franchise_id)
        status = "dissolved" if franchise_id in dissolved_ids else "active_or_continuing"
        if franchise is None:
            missing_rows.append((franchise_id, row, status))
            continue
        for field in ("name", "original_code", "current_code"):
            if getattr(franchise, field) != row[field]:
                setattr(franchise, field, row[field])
                changed += 1
        if not franchise.metadata_json:
            franchise.metadata_json = _source_metadata(franchise_id, status)
            changed += 1
    session.flush()
    for franchise_id, row, status in missing_rows:
        session.add(
            Franchise(
                id=franchise_id,
                name=row["name"],
                original_code=row["original_code"],
                current_code=row["current_code"],
                metadata_json=_source_metadata(franchise_id, status),
            )
        )
        changed += 1
    return changed


def _upsert_team(session, team_id: str, row: dict[str, Any], *, create_missing: bool) -> int:
    team = session.get(Team, team_id)
    if team is None:
        if not create_missing:
            return 0
        session.add(Team(team_id=team_id, **row))
        return 1

    changed = 0
    for field in ("team_name", "team_short_name", "city", "founded_year", "stadium_name", "franchise_id", "is_active"):
        if getattr(team, field) != row[field]:
            setattr(team, field, row[field])
            changed += 1
    current_aliases = _merge_aliases(team.aliases, [])
    aliases = _merge_aliases(team.aliases, row.get("aliases") or [])
    if current_aliases != aliases:
        team.aliases = aliases
        changed += 1
    return changed


def _upsert_teams(session) -> int:
    changed = 0
    for team_id, row in TEAM_ROWS.items():
        changed += _upsert_team(session, team_id, row, create_missing=True)
    for team_id, row in OPTIONAL_ALIAS_ROWS.items():
        changed += _upsert_team(session, team_id, row, create_missing=False)
    return changed


def _desired_history(max_year: int) -> dict[tuple[int, str], dict[str, Any]]:
    desired: dict[tuple[int, str], dict[str, Any]] = {}
    for entry in iter_team_history():
        end_year = entry.end_season if entry.end_season is not None else max_year
        if end_year > max_year:
            end_year = max_year
        for season in range(entry.start_season, end_year + 1):
            team = TEAM_ROWS[entry.team_code]
            desired[(season, entry.team_code)] = {
                "franchise_id": entry.franchise_id,
                "season": season,
                "team_code": entry.team_code,
                "team_name": team["team_name"],
                "city": team["city"],
                "stadium": team["stadium_name"],
            }
    return desired


def _sync_team_history(session, max_year: int) -> tuple[int, int]:
    desired = _desired_history(max_year)
    managed_codes = {code for _season, code in desired}
    existing = {
        (row.season, row.team_code): row
        for row in session.query(TeamHistory).filter(TeamHistory.team_code.in_(managed_codes)).all()
    }

    deleted = 0
    for key, row in list(existing.items()):
        if key not in desired:
            session.delete(row)
            deleted += 1

    changed = 0
    for key, row in desired.items():
        history = existing.get(key)
        if history is None:
            session.add(TeamHistory(**row))
            changed += 1
            continue
        for field, value in row.items():
            if getattr(history, field) != value:
                setattr(history, field, value)
                changed += 1
    return changed, deleted


def _sync_team_code_map(session, max_year: int) -> int:
    session.query(TeamCodeMap).delete()
    rows = []
    for entry in iter_team_history():
        canonical = FRANCHISE_CANONICAL_CODE[entry.franchise_id]
        end_year = entry.end_season if entry.end_season is not None else max_year
        if end_year > max_year:
            end_year = max_year
        for season in range(entry.start_season, end_year + 1):
            rows.append(
                TeamCodeMap(
                    franchise_id=entry.franchise_id,
                    season=season,
                    curr_code=entry.team_code,
                    canonical_code=canonical,
                    is_canonical=entry.team_code == canonical,
                )
            )
    session.bulk_save_objects(rows)
    return len(rows)


def _table_columns(inspector, table_name: str) -> set[str]:
    if table_name not in inspector.get_table_names():
        return set()
    return {column["name"] for column in inspector.get_columns(table_name)}


def _sqlite_year_expr(conn, column_name: str) -> str:
    if conn.dialect.name == "sqlite":
        return f"CAST(strftime('%Y', {column_name}) AS INTEGER)"
    return f"CAST(EXTRACT(YEAR FROM {column_name}) AS INTEGER)"


def _identity_for_code(code: Any, season: Any) -> tuple[int | None, str | None]:
    if code is None or season is None:
        return None, None
    raw = str(code).strip().upper()
    if not raw:
        return None, None
    try:
        year = int(season)
    except (TypeError, ValueError):
        return None, None
    normalized = team_code_from_game_id_segment(raw, year) or raw
    entry = find_team_history_entry(normalized, year)
    if entry is None:
        return None, None
    return entry.franchise_id, FRANCHISE_CANONICAL_CODE.get(entry.franchise_id)


def _distinct_predicate(conn, column: str, param_name: str) -> str:
    if conn.dialect.name == "postgresql":
        return f"{column} IS DISTINCT FROM :{param_name}"
    return (
        f"({column} != :{param_name} OR "
        f"({column} IS NULL AND :{param_name} IS NOT NULL) OR "
        f"({column} IS NOT NULL AND :{param_name} IS NULL))"
    )


def _update_fact_identity_postgres(
    conn,
    table: str,
    year_expr: str,
    team_column: str,
    set_columns: list[str],
    *,
    apply: bool,
) -> int:
    assignments = []
    distinct_predicates = []
    if "franchise_id" in set_columns:
        assignments.append("franchise_id = m.franchise_id")
        distinct_predicates.append("f.franchise_id IS DISTINCT FROM m.franchise_id")
    if "canonical_team_code" in set_columns:
        assignments.append("canonical_team_code = m.canonical_code")
        distinct_predicates.append("f.canonical_team_code IS DISTINCT FROM m.canonical_code")
    if not assignments:
        return 0

    match_clause = f"m.season = {year_expr} AND m.curr_code = f.{team_column} AND ({' OR '.join(distinct_predicates)})"
    if apply:
        result = conn.execute(
            text(
                f"""
                UPDATE {table} AS f
                SET {", ".join(assignments)}
                FROM team_code_map AS m
                WHERE {match_clause}
                """
            )
        )
        return int(result.rowcount or 0)
    return int(
        conn.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {table} AS f
                JOIN team_code_map AS m ON {match_clause}
                """
            )
        ).scalar()
        or 0
    )


def _update_fact_identity(
    conn, table: str, year_expr: str, team_column: str, *, apply: bool, postgres_year_expr: str | None = None
) -> int:
    inspector = inspect(conn)
    columns = _table_columns(inspector, table)
    set_columns = [column for column in ("franchise_id", "canonical_team_code") if column in columns]
    if team_column not in columns or not set_columns:
        return 0
    if conn.dialect.name == "postgresql" and postgres_year_expr:
        return _update_fact_identity_postgres(conn, table, postgres_year_expr, team_column, set_columns, apply=apply)

    pairs = conn.execute(
        text(
            f"""
            SELECT {year_expr} AS season_year, {team_column} AS team_code, COUNT(*) AS row_count
            FROM {table}
            WHERE {team_column} IS NOT NULL
            GROUP BY {year_expr}, {team_column}
            """
        )
    ).fetchall()
    changed = 0
    for season_year, team_code, row_count in pairs:
        franchise_id, canonical = _identity_for_code(team_code, season_year)
        if franchise_id is None:
            continue
        assignments = []
        distinct_predicates = []
        params: dict[str, Any] = {"season_year": season_year, "team_code": team_code}
        if "franchise_id" in set_columns:
            assignments.append("franchise_id = :franchise_id")
            distinct_predicates.append(_distinct_predicate(conn, "franchise_id", "franchise_id"))
            params["franchise_id"] = franchise_id
        if "canonical_team_code" in set_columns:
            assignments.append("canonical_team_code = :canonical_team_code")
            distinct_predicates.append(_distinct_predicate(conn, "canonical_team_code", "canonical_team_code"))
            params["canonical_team_code"] = canonical

        where_clause = (
            f"{year_expr} = :season_year AND {team_column} = :team_code AND ({' OR '.join(distinct_predicates)})"
        )
        if apply:
            result = conn.execute(
                text(f"UPDATE {table} SET {', '.join(assignments)} WHERE {where_clause}"),
                params,
            )
            changed += int(result.rowcount or 0)
        else:
            changed += int(
                conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE {where_clause}"), params).scalar() or 0
            )
    return changed


def _update_game_franchise_ids(conn, *, apply: bool) -> int:
    inspector = inspect(conn)
    columns = _table_columns(inspector, "game")
    if "game_id" not in columns:
        return 0
    year_expr = "CAST(SUBSTR(game_id, 1, 4) AS INTEGER)"
    changed = 0
    for team_column, franchise_column in GAME_TEAM_COLUMNS:
        if team_column not in columns or franchise_column not in columns:
            continue
        if conn.dialect.name == "postgresql":
            match_clause = (
                f"m.season = CAST(SUBSTR(g.game_id, 1, 4) AS INTEGER) "
                f"AND m.curr_code = g.{team_column} "
                f"AND g.{franchise_column} IS DISTINCT FROM m.franchise_id"
            )
            if apply:
                result = conn.execute(
                    text(
                        f"""
                        UPDATE game AS g
                        SET {franchise_column} = m.franchise_id
                        FROM team_code_map AS m
                        WHERE {match_clause}
                        """
                    )
                )
                changed += int(result.rowcount or 0)
            else:
                changed += int(
                    conn.execute(
                        text(
                            f"""
                            SELECT COUNT(*)
                            FROM game AS g
                            JOIN team_code_map AS m ON {match_clause}
                            """
                        )
                    ).scalar()
                    or 0
                )
            continue
        pairs = conn.execute(
            text(
                f"""
                SELECT {year_expr} AS season_year, {team_column} AS team_code, COUNT(*) AS row_count
                FROM game
                WHERE {team_column} IS NOT NULL
                GROUP BY {year_expr}, {team_column}
                """
            )
        ).fetchall()
        for season_year, team_code, row_count in pairs:
            franchise_id, _canonical = _identity_for_code(team_code, season_year)
            if franchise_id is None:
                continue
            params = {"season_year": season_year, "team_code": team_code, "franchise_id": franchise_id}
            where_clause = (
                f"{year_expr} = :season_year AND {team_column} = :team_code "
                f"AND {_distinct_predicate(conn, franchise_column, 'franchise_id')}"
            )
            if apply:
                result = conn.execute(
                    text(f"UPDATE game SET {franchise_column} = :franchise_id WHERE {where_clause}"),
                    params,
                )
                changed += int(result.rowcount or 0)
            else:
                changed += int(
                    conn.execute(text(f"SELECT COUNT(*) FROM game WHERE {where_clause}"), params).scalar() or 0
                )
    return changed


def _backfill_fact_identity(engine, *, apply: bool) -> int:
    changed = 0
    fact_specs = [
        (table, f"CAST({season_column} AS INTEGER)", f"CAST(f.{season_column} AS INTEGER)", team_column)
        for table, season_column, team_column in DIRECT_FACT_TABLES
    ]
    with engine.connect() as conn:
        fact_specs.extend(
            (
                table,
                _sqlite_year_expr(conn, date_column),
                f"CAST(EXTRACT(YEAR FROM f.{date_column}) AS INTEGER)",
                team_column,
            )
            for table, date_column, team_column in DATE_FACT_TABLES
        )
        fact_specs.extend(
            (
                table,
                f"CAST(SUBSTR({game_id_column}, 1, 4) AS INTEGER)",
                f"CAST(SUBSTR(f.{game_id_column}, 1, 4) AS INTEGER)",
                team_column,
            )
            for table, game_id_column, team_column in GAME_ID_FACT_TABLES
        )

    for table, year_expr, postgres_year_expr, team_column in fact_specs:
        with engine.begin() as conn:
            changed += _update_fact_identity(
                conn, table, year_expr, team_column, apply=apply, postgres_year_expr=postgres_year_expr
            )
    with engine.begin() as conn:
        changed += _update_game_franchise_ids(conn, apply=apply)
    return changed


def _reset_postgres_sequences(engine) -> None:
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        for table, column in (("team_franchises", "id"), ("team_history", "id"), ("team_code_map", "id")):
            sequence_name = conn.execute(
                text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
                {"table_name": table, "column_name": column},
            ).scalar()
            if not sequence_name:
                continue
            conn.execute(
                text(f"SELECT setval(:sequence_name, COALESCE((SELECT MAX({column}) FROM {table}), 1))"),
                {"sequence_name": sequence_name},
            )


def _install_postgres_team_code_normalizer(engine, *, apply: bool) -> None:
    if engine.dialect.name != "postgresql" or not apply:
        return
    with engine.begin() as conn:
        conn.execute(text(POSTGRES_RESOLVE_TEAM_CODE_SQL))
        conn.execute(text(POSTGRES_NORMALIZE_TEAM_CODE_COLUMN_SQL))


def run(
    *, db_url: str = DEFAULT_DB_URL, max_year: int | None = None, apply: bool = True, include_facts: bool = True
) -> dict[str, int]:
    max_year = max_year or datetime.now().year
    engine = create_engine(db_url, pool_pre_ping=True)
    _install_postgres_team_code_normalizer(engine, apply=apply)
    Session = sessionmaker(bind=engine)
    result = {
        "franchise_changes": 0,
        "team_changes": 0,
        "team_history_changes": 0,
        "team_history_deleted": 0,
        "team_code_map_rows": 0,
        "fact_identity_rows": 0,
    }

    session = Session()
    try:
        result["franchise_changes"] = _upsert_franchises(session)
        result["team_changes"] = _upsert_teams(session)
        history_changes, history_deleted = _sync_team_history(session, max_year)
        result["team_history_changes"] = history_changes
        result["team_history_deleted"] = history_deleted
        result["team_code_map_rows"] = _sync_team_code_map(session, max_year)
        if apply:
            session.commit()
        else:
            session.rollback()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    if include_facts:
        result["fact_identity_rows"] = _backfill_fact_identity(engine, apply=apply)

    if apply:
        _reset_postgres_sequences(engine)

    engine.dispose()
    return result


def _mask_url(db_url: str) -> str:
    parts = urlsplit(db_url)
    if not parts.password:
        return db_url
    username = parts.username or ""
    hostname = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    netloc = f"{username}:***@{hostname}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair historical team metadata and fact identity columns.")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--max-year", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-facts", action="store_true")
    parser.add_argument(
        "--include-oci", action="store_true", help="Also repair OCI_DB_URL after the local/default target."
    )
    parser.add_argument("--oci-only", action="store_true", help="Repair only OCI_DB_URL.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    urls = [] if args.oci_only else [args.db_url]
    if args.include_oci or args.oci_only:
        oci_url = os.getenv("OCI_DB_URL")
        if oci_url:
            urls.append(oci_url)
        else:
            raise SystemExit("OCI_DB_URL is not set")

    for url in urls:
        result = run(db_url=url, max_year=args.max_year, apply=not args.dry_run, include_facts=not args.skip_facts)
        mode = "dry_run" if args.dry_run else "applied"
        print(f"{mode}: {_mask_url(url)}")
        for key, value in result.items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
