#!/usr/bin/env python3
"""Fail when managed KBO team codes appear outside their valid seasons."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import logging

from src.utils.team_history import iter_team_history

logger = logging.getLogger(__name__)
DEFAULT_DB_URL = "sqlite:///./data/kbo_dev.db"
MANAGED_CODES = {entry.team_code.upper() for entry in iter_team_history()}
VALID_RANGES = tuple(iter_team_history())

DIRECT_TABLES = (
    ("player_season_batting", "season", "team_code"),
    ("player_season_pitching", "season", "team_code"),
    ("player_season_fielding", "year", "team_id"),
    ("player_season_baserunning", "year", "team_id"),
    ("team_season_batting", "season", "team_id"),
    ("team_season_pitching", "season", "team_id"),
    ("team_history", "season", "team_code"),
    ("team_code_map", "season", "curr_code"),
)
DATE_TABLES = (("team_daily_roster", "roster_date", "team_code"),)
GAME_ID_TABLES = (
    ("game", "game_id", "away_team"),
    ("game", "game_id", "home_team"),
    ("game", "game_id", "winning_team"),
    ("game_inning_scores", "game_id", "team_code"),
    ("game_lineups", "game_id", "team_code"),
    ("game_batting_stats", "game_id", "team_code"),
    ("game_pitching_stats", "game_id", "team_code"),
)


def _columns(inspector, table: str) -> set[str]:
    if table not in inspector.get_table_names():
        return set()
    return {column["name"] for column in inspector.get_columns(table)}


def _date_year_expr(conn, column: str) -> str:
    if conn.dialect.name == "sqlite":
        return f"CAST(strftime('%Y', {column}) AS INTEGER)"
    return f"CAST(EXTRACT(YEAR FROM {column}) AS INTEGER)"


def _valid_for_year(code: str, year: int) -> bool:
    raw = code.upper()
    for entry in VALID_RANGES:
        if entry.team_code.upper() != raw:
            continue
        end_year = entry.end_season if entry.end_season is not None else year
        if entry.start_season <= year <= end_year:
            return True
    return False


def _collect_table_issues(
    conn,
    table: str,
    year_expr: str,
    team_column: str,
    sample_limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            f"""
            SELECT {year_expr} AS season_year, {team_column} AS team_code, COUNT(*) AS row_count
            FROM {table}
            WHERE {team_column} IS NOT NULL
            GROUP BY {year_expr}, {team_column}
            """,
        ),
    ).fetchall()
    issues: list[dict[str, Any]] = []
    for season_year, team_code, row_count in rows:
        if season_year is None or team_code is None:
            continue
        code = str(team_code).strip().upper()
        if code not in MANAGED_CODES:
            continue
        try:
            year = int(season_year)
        except (TypeError, ValueError):
            continue
        if not _valid_for_year(code, year):
            issues.append(
                {
                    "table": table,
                    "column": team_column,
                    "season": year,
                    "team_code": code,
                    "row_count": int(row_count or 0),
                },
            )
            if len(issues) >= sample_limit:
                break
    return issues


def collect_issues(db_url: str, sample_limit: int = 50) -> list[dict[str, Any]]:
    engine = create_engine(db_url)
    issues: list[dict[str, Any]] = []
    with engine.connect() as conn:
        inspector = inspect(conn)
        for table, season_column, team_column in DIRECT_TABLES:
            columns = _columns(inspector, table)
            if {season_column, team_column} <= columns:
                issues.extend(
                    _collect_table_issues(conn, table, f"CAST({season_column} AS INTEGER)", team_column, sample_limit),
                )
        for table, date_column, team_column in DATE_TABLES:
            columns = _columns(inspector, table)
            if {date_column, team_column} <= columns:
                issues.extend(
                    _collect_table_issues(conn, table, _date_year_expr(conn, date_column), team_column, sample_limit),
                )
        for table, game_id_column, team_column in GAME_ID_TABLES:
            columns = _columns(inspector, table)
            if {game_id_column, team_column} <= columns:
                issues.extend(
                    _collect_table_issues(
                        conn,
                        table,
                        f"CAST(SUBSTR({game_id_column}, 1, 4) AS INTEGER)",
                        team_column,
                        sample_limit,
                    ),
                )
    engine.dispose()
    return issues[:sample_limit]


def _resolve_db_url(raw_url: str) -> str:
    if raw_url.startswith("env:"):
        env_name = raw_url.removeprefix("env:")
        value = os.getenv(env_name)
        if not value:
            msg = f"Environment variable {env_name} is not set"
            raise ValueError(msg)
        return value
    return raw_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit KBO team-code season validity.")
    parser.add_argument("--db-url", default=DEFAULT_DB_URL, help="Database URL. Supports env:VAR_NAME.")
    parser.add_argument("--sample-limit", type=int, default=50)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    issues = collect_issues(_resolve_db_url(args.db_url), sample_limit=args.sample_limit)
    if args.json:
        logger.info(json.dumps({"ok": not issues, "issues": issues}, ensure_ascii=False, indent=2))
    elif issues:
        logger.info("FAIL: team codes outside valid seasons")
        for issue in issues:
            logger.info(
                f"  {issue['table']}.{issue['column']} {issue['season']} {issue['team_code']} rows={issue['row_count']}",
            )
    else:
        logger.info("PASS: all managed team codes are valid for their seasons")
    if issues:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
