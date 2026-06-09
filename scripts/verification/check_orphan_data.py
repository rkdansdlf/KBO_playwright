#!/usr/bin/env python3
"""Verify referential integrity gaps that SQLite FK checks can miss."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)
DEFAULT_DB_PATH = Path("data/kbo_dev.db")


@dataclass
class CheckResult:
    name: str
    status: str
    row_count: int = 0
    distinct_count: int = 0
    samples: list[Any] | None = None
    severity: str = "fail"
    error: str | None = None

    @property
    def failed(self) -> bool:
        return self.status in {"FAIL", "ERROR"} and self.severity == "fail"


EXPECTED_FKS = (
    ("players", "player_basic_id", "player_basic", "player_id"),
    ("game_metadata", "game_id", "game", "game_id"),
    ("game_batting_stats", "game_id", "game", "game_id"),
    ("game_batting_stats", "player_id", "player_basic", "player_id"),
    ("game_pitching_stats", "game_id", "game", "game_id"),
    ("game_pitching_stats", "player_id", "player_basic", "player_id"),
    ("game_lineups", "game_id", "game", "game_id"),
    ("game_lineups", "player_id", "player_basic", "player_id"),
    ("game_events", "game_id", "game", "game_id"),
    ("game_events", "batter_id", "player_basic", "player_id"),
    ("game_events", "pitcher_id", "player_basic", "player_id"),
    ("game_summary", "game_id", "game", "game_id"),
    ("game_summary", "player_id", "player_basic", "player_id"),
    ("game_play_by_play", "game_id", "game", "game_id"),
    ("game_inning_scores", "game_id", "game", "game_id"),
    ("game_id_aliases", "canonical_game_id", "game", "game_id"),
    ("player_season_batting", "player_id", "player_basic", "player_id"),
    ("player_season_batting", "team_code", "teams", "team_id"),
    ("player_season_pitching", "player_id", "player_basic", "player_id"),
    ("player_season_pitching", "team_code", "teams", "team_id"),
    ("team_daily_roster", "team_code", "teams", "team_id"),
    ("team_daily_roster", "player_basic_id", "player_basic", "player_id"),
    ("player_movements", "canonical_team_id", "teams", "team_id"),
    ("player_movements", "player_basic_id", "player_basic", "player_id"),
    ("matchup_bvp", "batter_id", "player_basic", "player_id"),
    ("matchup_bvp", "pitcher_id", "player_basic", "player_id"),
    ("matchup_batter_splits", "player_id", "player_basic", "player_id"),
    ("matchup_pitcher_splits", "player_id", "player_basic", "player_id"),
    ("matchup_batter_team_split", "player_id", "player_basic", "player_id"),
    ("matchup_pitcher_team_split", "player_id", "player_basic", "player_id"),
    ("matchup_batter_stadium_split", "player_id", "player_basic", "player_id"),
    ("matchup_batter_vs_starter", "player_id", "player_basic", "player_id"),
)

EXPECTED_CASCADE_FKS = (
    ("game_metadata", "game_id", "game", "game_id"),
    ("game_batting_stats", "game_id", "game", "game_id"),
    ("game_pitching_stats", "game_id", "game", "game_id"),
    ("game_lineups", "game_id", "game", "game_id"),
    ("game_events", "game_id", "game", "game_id"),
    ("game_summary", "game_id", "game", "game_id"),
    ("game_play_by_play", "game_id", "game", "game_id"),
    ("game_inning_scores", "game_id", "game", "game_id"),
    ("game_id_aliases", "canonical_game_id", "game", "game_id"),
)

REQUIRED_INTEGRITY_COLUMNS = (
    ("players", {"player_basic_id"}),
    ("team_daily_roster", {"person_type", "player_basic_id"}),
    ("player_movements", {"canonical_team_id", "player_basic_id", "resolution_status"}),
)

ROSTER_PLAYER_POSITIONS = ("투수", "포수", "내야수", "외야수")
ROSTER_STAFF_POSITIONS = ("감독", "코치")


def _sqlite_path_from_url(db_url: str) -> Path:
    if db_url == "sqlite:///:memory:":
        raise ValueError(":memory: database URLs are not supported by this CLI")
    if not db_url.startswith("sqlite:///"):
        raise ValueError(f"Only sqlite:/// URLs are supported, got: {db_url}")
    raw_path = db_url.removeprefix("sqlite:///")
    return Path(raw_path)


def _mask_url(db_url: str) -> str:
    parts = urlsplit(db_url)
    if not parts.password:
        return db_url
    username = parts.username or ""
    hostname = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    netloc = f"{username}:***@{hostname}{port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _resolve_db_url(raw_url: str | None) -> str | None:
    if not raw_url:
        return None
    if raw_url.startswith("env:"):
        env_name = raw_url.removeprefix("env:")
        value = os.getenv(env_name)
        if not value:
            raise ValueError(f"Environment variable {env_name} is not set")
        return value
    return raw_url


def _resolve_db_path(args: argparse.Namespace) -> Path:
    if args.db_path and args.db_url:
        raise ValueError("Use either --db-path or --db-url, not both")
    if args.db_url:
        return _sqlite_path_from_url(args.db_url)
    if args.db_path:
        return Path(args.db_path)
    return DEFAULT_DB_PATH


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _has_columns(conn: sqlite3.Connection, table_name: str, required: set[str]) -> bool:
    return required <= _columns(conn, table_name)


def _scalar(conn: sqlite3.Connection, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    return int(row[0] or 0) if row else 0


def _run_count_check(
    conn: sqlite3.Connection,
    *,
    name: str,
    base_sql: str,
    distinct_expr: str,
    sample_expr: str,
    sample_limit: int,
    severity: str = "fail",
) -> CheckResult:
    try:
        row_count = _scalar(conn, f"SELECT COUNT(*) {base_sql}")
        distinct_count = 0
        samples: list[Any] = []
        if row_count:
            distinct_count = _scalar(conn, f"SELECT COUNT(DISTINCT {distinct_expr}) {base_sql}")
            sample_sql = f"SELECT DISTINCT {sample_expr} {base_sql} LIMIT {int(sample_limit)}"
            samples = [row[0] for row in conn.execute(sample_sql).fetchall()]
        if row_count == 0:
            status = "PASS"
        elif severity == "warning":
            status = "WARN"
        else:
            status = "FAIL"
        return CheckResult(
            name=name,
            status=status,
            row_count=row_count,
            distinct_count=distinct_count,
            samples=samples,
            severity=severity,
        )
    except sqlite3.Error as exc:
        return CheckResult(name=name, status="ERROR", severity=severity, error=str(exc))


def _foreign_key_check(conn: sqlite3.Connection, sample_limit: int) -> CheckResult:
    rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    samples = [
        {
            "table": row[0],
            "rowid": row[1],
            "parent": row[2],
            "fk_index": row[3],
        }
        for row in rows[:sample_limit]
    ]
    return CheckResult(
        name="PRAGMA foreign_key_check",
        status="FAIL" if rows else "PASS",
        row_count=len(rows),
        distinct_count=len({row[0] for row in rows}),
        samples=samples,
    )


def _declared_fk_report(conn: sqlite3.Connection) -> CheckResult:
    missing = []
    for child_table, child_col, parent_table, parent_col in EXPECTED_FKS:
        if not _has_columns(conn, child_table, {child_col}) or not _has_columns(conn, parent_table, {parent_col}):
            continue
        fk_rows = conn.execute(f"PRAGMA foreign_key_list({child_table})").fetchall()
        has_fk = any(row[2] == parent_table and row[3] == child_col and row[4] == parent_col for row in fk_rows)
        if not has_fk:
            missing.append(f"{child_table}.{child_col}->{parent_table}.{parent_col}")
    return CheckResult(
        name="SQLite declared FK coverage",
        status="WARN" if missing else "PASS",
        row_count=len(missing),
        distinct_count=len(missing),
        samples=missing[:20],
        severity="warning",
    )


def _cascade_fk_report(conn: sqlite3.Connection) -> CheckResult:
    missing = []
    for child_table, child_col, parent_table, parent_col in EXPECTED_CASCADE_FKS:
        if not _has_columns(conn, child_table, {child_col}) or not _has_columns(conn, parent_table, {parent_col}):
            continue
        fk_rows = conn.execute(f"PRAGMA foreign_key_list({child_table})").fetchall()
        has_cascade = any(
            row[2] == parent_table and row[3] == child_col and row[4] == parent_col and str(row[6]).upper() == "CASCADE"
            for row in fk_rows
        )
        if not has_cascade:
            missing.append(f"{child_table}.{child_col}->{parent_table}.{parent_col} ON DELETE CASCADE")
    return CheckResult(
        name="SQLite game child cascade coverage",
        status="FAIL" if missing else "PASS",
        row_count=len(missing),
        distinct_count=len(missing),
        samples=missing[:20],
    )


def _integrity_column_report(conn: sqlite3.Connection) -> CheckResult:
    missing = []
    for table_name, required_columns in REQUIRED_INTEGRITY_COLUMNS:
        if not _table_exists(conn, table_name):
            continue
        missing_columns = sorted(required_columns - _columns(conn, table_name))
        if missing_columns:
            missing.append(f"{table_name}: {', '.join(missing_columns)}")
    return CheckResult(
        name="Canonical integrity column coverage",
        status="FAIL" if missing else "PASS",
        row_count=len(missing),
        distinct_count=len(missing),
        samples=missing[:20],
    )


def _unknown_player_predicate(alias: str) -> str:
    return f"UPPER(TRIM({alias}.name)) LIKE 'UNKNOWN %' AND SUBSTR(UPPER(TRIM({alias}.name)), 9) GLOB '[0-9]*'"


def _sa_table_exists(conn: Connection, table_name: str) -> bool:
    return inspect(conn).has_table(table_name)


def _sa_columns(conn: Connection, table_name: str) -> set[str]:
    if not _sa_table_exists(conn, table_name):
        return set()
    return {column["name"] for column in inspect(conn).get_columns(table_name)}


def _sa_has_columns(conn: Connection, table_name: str, required: set[str]) -> bool:
    return required <= _sa_columns(conn, table_name)


def _sa_scalar(conn: Connection, sql: str) -> int:
    row = conn.execute(text(sql)).first()
    return int(row[0] or 0) if row else 0


def _sa_unknown_player_predicate(alias: str, dialect: str) -> str:
    name_expr = f"UPPER(TRIM({alias}.name))"
    if dialect == "postgresql":
        return f"{name_expr} ~ '^UNKNOWN [0-9]+$'"
    return _unknown_player_predicate(alias)


def _sa_run_count_check(
    conn: Connection,
    *,
    name: str,
    base_sql: str,
    distinct_expr: str,
    sample_expr: str,
    sample_limit: int,
    severity: str = "fail",
) -> CheckResult:
    try:
        row_count = _sa_scalar(conn, f"SELECT COUNT(*) {base_sql}")
        distinct_count = 0
        samples: list[Any] = []
        if row_count:
            distinct_count = _sa_scalar(conn, f"SELECT COUNT(DISTINCT {distinct_expr}) {base_sql}")
            sample_sql = f"SELECT DISTINCT {sample_expr} {base_sql} LIMIT {int(sample_limit)}"
            samples = [row[0] for row in conn.execute(text(sample_sql)).fetchall()]
        if row_count == 0:
            status = "PASS"
        elif severity == "warning":
            status = "WARN"
        else:
            status = "FAIL"
        return CheckResult(
            name=name,
            status=status,
            row_count=row_count,
            distinct_count=distinct_count,
            samples=samples,
            severity=severity,
        )
    except Exception as exc:  # noqa: BLE001
        return CheckResult(name=name, status="ERROR", severity=severity, error=str(exc))


def _sa_declared_fk_report(conn: Connection) -> CheckResult:
    missing = []
    inspector = inspect(conn)
    for child_table, child_col, parent_table, parent_col in EXPECTED_FKS:
        if not _sa_has_columns(conn, child_table, {child_col}) or not _sa_has_columns(conn, parent_table, {parent_col}):
            continue
        fk_rows = inspector.get_foreign_keys(child_table)
        has_fk = any(
            fk.get("referred_table") == parent_table
            and child_col in (fk.get("constrained_columns") or [])
            and parent_col in (fk.get("referred_columns") or [])
            for fk in fk_rows
        )
        if not has_fk:
            missing.append(f"{child_table}.{child_col}->{parent_table}.{parent_col}")
    return CheckResult(
        name="Declared FK coverage",
        status="WARN" if missing else "PASS",
        row_count=len(missing),
        distinct_count=len(missing),
        samples=missing[:20],
        severity="warning",
    )


def _sa_cascade_fk_report(conn: Connection) -> CheckResult:
    missing = []
    inspector = inspect(conn)
    for child_table, child_col, parent_table, parent_col in EXPECTED_CASCADE_FKS:
        if not _sa_has_columns(conn, child_table, {child_col}) or not _sa_has_columns(conn, parent_table, {parent_col}):
            continue
        fk_rows = inspector.get_foreign_keys(child_table)
        has_cascade = False
        for fk in fk_rows:
            options = fk.get("options") or {}
            ondelete = str(options.get("ondelete") or options.get("on_delete") or "").upper()
            if (
                fk.get("referred_table") == parent_table
                and child_col in (fk.get("constrained_columns") or [])
                and parent_col in (fk.get("referred_columns") or [])
                and ondelete == "CASCADE"
            ):
                has_cascade = True
                break
        if not has_cascade:
            missing.append(f"{child_table}.{child_col}->{parent_table}.{parent_col} ON DELETE CASCADE")
    return CheckResult(
        name="Game child cascade coverage",
        status="FAIL" if missing else "PASS",
        row_count=len(missing),
        distinct_count=len(missing),
        samples=missing[:20],
    )


def _sa_integrity_column_report(conn: Connection) -> CheckResult:
    missing = []
    for table_name, required_columns in REQUIRED_INTEGRITY_COLUMNS:
        if not _sa_table_exists(conn, table_name):
            continue
        missing_columns = sorted(required_columns - _sa_columns(conn, table_name))
        if missing_columns:
            missing.append(f"{table_name}: {', '.join(missing_columns)}")
    return CheckResult(
        name="Canonical integrity column coverage",
        status="FAIL" if missing else "PASS",
        row_count=len(missing),
        distinct_count=len(missing),
        samples=missing[:20],
    )


def _sa_add_check(
    checks: list[dict[str, str]],
    conn: Connection,
    *,
    table: str,
    required: set[str],
    name: str,
    base_sql: str,
    distinct_expr: str,
    sample_expr: str,
) -> None:
    if _sa_has_columns(conn, table, required):
        checks.append(
            {
                "name": name,
                "base_sql": base_sql,
                "distinct_expr": distinct_expr,
                "sample_expr": sample_expr,
            }
        )


def _sa_targeted_checks(conn: Connection, sample_limit: int) -> list[CheckResult]:
    checks: list[dict[str, str]] = []
    dialect = conn.dialect.name

    for table_name in (
        "game_batting_stats",
        "game_pitching_stats",
        "game_metadata",
        "game_inning_scores",
        "game_lineups",
        "game_events",
        "game_summary",
        "game_play_by_play",
    ):
        _sa_add_check(
            checks,
            conn,
            table=table_name,
            required={"game_id"},
            name=f"{table_name} -> game",
            base_sql=f"FROM {table_name} AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL",
            distinct_expr="t.game_id",
            sample_expr="t.game_id",
        )
    _sa_add_check(
        checks,
        conn,
        table="game_id_aliases",
        required={"canonical_game_id"},
        name="game_id_aliases -> game",
        base_sql=(
            "FROM game_id_aliases AS t LEFT JOIN game AS p ON t.canonical_game_id = p.game_id WHERE p.game_id IS NULL"
        ),
        distinct_expr="t.canonical_game_id",
        sample_expr="t.canonical_game_id",
    )

    unknown_predicate = _sa_unknown_player_predicate("p", dialect)
    for table_name in ("player_season_batting", "player_season_pitching"):
        _sa_add_check(
            checks,
            conn,
            table=table_name,
            required={"player_id"},
            name=f"{table_name} -> player_basic",
            base_sql=f"FROM {table_name} AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL",
            distinct_expr="t.player_id",
            sample_expr="t.player_id",
        )
        _sa_add_check(
            checks,
            conn,
            table=table_name,
            required={"player_id"},
            name=f"{table_name} -> Unknown player_basic stubs",
            base_sql=f"FROM {table_name} AS t JOIN player_basic AS p ON t.player_id = p.player_id WHERE {unknown_predicate}",
            distinct_expr="t.player_id",
            sample_expr="t.player_id",
        )

    for table_name in ("game_batting_stats", "game_pitching_stats", "game_lineups"):
        _sa_add_check(
            checks,
            conn,
            table=table_name,
            required={"player_id"},
            name=f"{table_name}.player_id -> player_basic",
            base_sql=(
                f"FROM {table_name} AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id "
                "WHERE t.player_id IS NOT NULL AND p.player_id IS NULL"
            ),
            distinct_expr="t.player_id",
            sample_expr="t.player_id",
        )

    for table_name, column in (
        ("game_events", "batter_id"),
        ("game_events", "pitcher_id"),
        ("game_summary", "player_id"),
        ("matchup_bvp", "batter_id"),
        ("matchup_bvp", "pitcher_id"),
        ("matchup_batter_splits", "player_id"),
        ("matchup_pitcher_splits", "player_id"),
        ("matchup_batter_team_split", "player_id"),
        ("matchup_pitcher_team_split", "player_id"),
        ("matchup_batter_stadium_split", "player_id"),
        ("matchup_batter_vs_starter", "player_id"),
        ("team_daily_roster", "player_basic_id"),
        ("player_movements", "player_basic_id"),
        ("players", "player_basic_id"),
    ):
        _sa_add_check(
            checks,
            conn,
            table=table_name,
            required={column},
            name=f"{table_name}.{column} -> player_basic",
            base_sql=(
                f"FROM {table_name} AS t LEFT JOIN player_basic AS p ON t.{column} = p.player_id "
                f"WHERE t.{column} IS NOT NULL AND p.player_id IS NULL"
            ),
            distinct_expr=f"t.{column}",
            sample_expr=f"t.{column}",
        )

    for column, name in (
        ("home_team", "Game home_team -> teams"),
        ("away_team", "Game away_team -> teams"),
        ("winning_team", "Game winning_team -> teams"),
    ):
        _sa_add_check(
            checks,
            conn,
            table="game",
            required={column},
            name=name,
            base_sql=f"FROM game AS t LEFT JOIN teams AS p ON t.{column} = p.team_id WHERE t.{column} IS NOT NULL AND p.team_id IS NULL",
            distinct_expr=f"t.{column}",
            sample_expr=f"t.{column}",
        )

    for table_name, column in (
        ("game_inning_scores", "team_code"),
        ("game_lineups", "team_code"),
        ("game_batting_stats", "team_code"),
        ("game_pitching_stats", "team_code"),
        ("player_season_batting", "team_code"),
        ("player_season_pitching", "team_code"),
        ("player_season_fielding", "team_id"),
        ("player_season_baserunning", "team_id"),
        ("team_season_batting", "team_id"),
        ("team_season_pitching", "team_id"),
        ("team_daily_roster", "team_code"),
        ("player_movements", "canonical_team_id"),
    ):
        _sa_add_check(
            checks,
            conn,
            table=table_name,
            required={column},
            name=f"{table_name}.{column} -> teams",
            base_sql=f"FROM {table_name} AS t LEFT JOIN teams AS p ON t.{column} = p.team_id WHERE t.{column} IS NOT NULL AND p.team_id IS NULL",
            distinct_expr=f"t.{column}",
            sample_expr=f"t.{column}",
        )

    if _sa_has_columns(conn, "team_daily_roster", {"position"}):
        checks.append(
            {
                "name": "team_daily_roster parser artifact positions",
                "base_sql": "FROM team_daily_roster AS t WHERE t.position IN ('포지션')",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
    if _sa_has_columns(conn, "team_daily_roster", {"person_type", "player_basic_id"}):
        checks.append(
            {
                "name": "team_daily_roster player rows require canonical player",
                "base_sql": "FROM team_daily_roster AS t WHERE t.person_type = 'player' AND t.player_basic_id IS NULL",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
    if _sa_has_columns(conn, "player_movements", {"canonical_team_id"}):
        checks.append(
            {
                "name": "player_movements require canonical team",
                "base_sql": "FROM player_movements AS t WHERE t.canonical_team_id IS NULL",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
    if _sa_has_columns(conn, "player_movements", {"resolution_status"}):
        checks.append(
            {
                "name": "player_movements unresolved player links",
                "base_sql": (
                    "FROM player_movements AS t WHERE t.resolution_status IN ('unresolved', 'unresolved_player')"
                ),
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
                "severity": "warning",
            }
        )
    if _sa_has_columns(conn, "players", {"kbo_person_id", "player_basic_id"}):
        if dialect == "postgresql":
            numeric_predicate = "t.kbo_person_id ~ '^[0-9]+$'"
            numeric_id_expr = "CASE WHEN t.kbo_person_id ~ '^[0-9]+$' THEN CAST(t.kbo_person_id AS INTEGER) END"
        else:
            numeric_predicate = "t.kbo_person_id <> '' AND t.kbo_person_id NOT GLOB '*[^0-9]*'"
            numeric_id_expr = "CAST(t.kbo_person_id AS INTEGER)"
        checks.append(
            {
                "name": "players mirror canonical player_basic_id mismatch",
                "base_sql": (
                    "FROM players AS t JOIN player_basic AS p "
                    f"ON {numeric_id_expr} = p.player_id WHERE {numeric_predicate} "
                    "AND (t.player_basic_id IS NULL OR t.player_basic_id <> p.player_id)"
                ),
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
        checks.append(
            {
                "name": "players legacy rows without player_basic mirror",
                "base_sql": "FROM players AS t WHERE t.player_basic_id IS NULL",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
                "severity": "warning",
            }
        )

    if _sa_has_columns(conn, "player_basic", {"player_id", "name"}):
        checks.append(
            {
                "name": "player_basic Unknown ID stubs",
                "base_sql": f"FROM player_basic AS p WHERE {_sa_unknown_player_predicate('p', dialect)}",
                "distinct_expr": "p.player_id",
                "sample_expr": "p.player_id",
            }
        )

    return [
        _sa_run_count_check(
            conn,
            name=check["name"],
            base_sql=check["base_sql"],
            distinct_expr=check["distinct_expr"],
            sample_expr=check["sample_expr"],
            sample_limit=sample_limit,
            severity=check.get("severity", "fail"),
        )
        for check in checks
    ]


def _add_check(
    checks: list[dict[str, str]],
    conn: sqlite3.Connection,
    *,
    table: str,
    required: set[str],
    name: str,
    base_sql: str,
    distinct_expr: str,
    sample_expr: str,
) -> None:
    if _has_columns(conn, table, required):
        checks.append(
            {
                "name": name,
                "base_sql": base_sql,
                "distinct_expr": distinct_expr,
                "sample_expr": sample_expr,
            }
        )


def _targeted_checks(conn: sqlite3.Connection, sample_limit: int) -> list[CheckResult]:
    checks: list[dict[str, str]] = []

    for table in (
        "game_batting_stats",
        "game_pitching_stats",
        "game_metadata",
        "game_inning_scores",
        "game_lineups",
        "game_events",
        "game_summary",
        "game_play_by_play",
    ):
        _add_check(
            checks,
            conn,
            table=table,
            required={"game_id"},
            name=f"{table} -> game",
            base_sql=f"FROM {table} AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL",
            distinct_expr="t.game_id",
            sample_expr="t.game_id",
        )
    _add_check(
        checks,
        conn,
        table="game_id_aliases",
        required={"canonical_game_id"},
        name="game_id_aliases -> game",
        base_sql=(
            "FROM game_id_aliases AS t LEFT JOIN game AS p ON t.canonical_game_id = p.game_id WHERE p.game_id IS NULL"
        ),
        distinct_expr="t.canonical_game_id",
        sample_expr="t.canonical_game_id",
    )

    for table in ("player_season_batting", "player_season_pitching"):
        _add_check(
            checks,
            conn,
            table=table,
            required={"player_id"},
            name=f"{table} -> player_basic",
            base_sql=f"FROM {table} AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL",
            distinct_expr="t.player_id",
            sample_expr="t.player_id",
        )
        _add_check(
            checks,
            conn,
            table=table,
            required={"player_id"},
            name=f"{table} -> Unknown player_basic stubs",
            base_sql=(
                f"FROM {table} AS t JOIN player_basic AS p ON t.player_id = p.player_id "
                f"WHERE {_unknown_player_predicate('p')}"
            ),
            distinct_expr="t.player_id",
            sample_expr="t.player_id",
        )

    for table in ("game_batting_stats", "game_pitching_stats", "game_lineups"):
        _add_check(
            checks,
            conn,
            table=table,
            required={"player_id"},
            name=f"{table}.player_id -> player_basic",
            base_sql=(
                f"FROM {table} AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id "
                "WHERE t.player_id IS NOT NULL AND p.player_id IS NULL"
            ),
            distinct_expr="t.player_id",
            sample_expr="t.player_id",
        )

    for table, column in (
        ("game_events", "batter_id"),
        ("game_events", "pitcher_id"),
        ("game_summary", "player_id"),
        ("matchup_bvp", "batter_id"),
        ("matchup_bvp", "pitcher_id"),
        ("matchup_batter_splits", "player_id"),
        ("matchup_pitcher_splits", "player_id"),
        ("matchup_batter_team_split", "player_id"),
        ("matchup_pitcher_team_split", "player_id"),
        ("matchup_batter_stadium_split", "player_id"),
        ("matchup_batter_vs_starter", "player_id"),
        ("team_daily_roster", "player_basic_id"),
        ("player_movements", "player_basic_id"),
        ("players", "player_basic_id"),
    ):
        _add_check(
            checks,
            conn,
            table=table,
            required={column},
            name=f"{table}.{column} -> player_basic",
            base_sql=(
                f"FROM {table} AS t LEFT JOIN player_basic AS p ON t.{column} = p.player_id "
                f"WHERE t.{column} IS NOT NULL AND p.player_id IS NULL"
            ),
            distinct_expr=f"t.{column}",
            sample_expr=f"t.{column}",
        )

    game_team_columns = (
        ("home_team", "Game home_team -> teams"),
        ("away_team", "Game away_team -> teams"),
        ("winning_team", "Game winning_team -> teams"),
    )
    for column, name in game_team_columns:
        _add_check(
            checks,
            conn,
            table="game",
            required={column},
            name=name,
            base_sql=f"FROM game AS t LEFT JOIN teams AS p ON t.{column} = p.team_id WHERE t.{column} IS NOT NULL AND p.team_id IS NULL",
            distinct_expr=f"t.{column}",
            sample_expr=f"t.{column}",
        )

    for table, column in (
        ("game_inning_scores", "team_code"),
        ("game_lineups", "team_code"),
        ("game_batting_stats", "team_code"),
        ("game_pitching_stats", "team_code"),
        ("player_season_batting", "team_code"),
        ("player_season_pitching", "team_code"),
        ("player_season_fielding", "team_id"),
        ("player_season_baserunning", "team_id"),
        ("team_season_batting", "team_id"),
        ("team_season_pitching", "team_id"),
        ("team_daily_roster", "team_code"),
        ("player_movements", "canonical_team_id"),
    ):
        _add_check(
            checks,
            conn,
            table=table,
            required={column},
            name=f"{table}.{column} -> teams",
            base_sql=f"FROM {table} AS t LEFT JOIN teams AS p ON t.{column} = p.team_id WHERE t.{column} IS NOT NULL AND p.team_id IS NULL",
            distinct_expr=f"t.{column}",
            sample_expr=f"t.{column}",
        )

    if _has_columns(conn, "team_daily_roster", {"position"}):
        checks.append(
            {
                "name": "team_daily_roster parser artifact positions",
                "base_sql": "FROM team_daily_roster AS t WHERE t.position IN ('포지션')",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
    if _has_columns(conn, "team_daily_roster", {"person_type", "player_basic_id"}):
        checks.append(
            {
                "name": "team_daily_roster player rows require canonical player",
                "base_sql": "FROM team_daily_roster AS t WHERE t.person_type = 'player' AND t.player_basic_id IS NULL",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
    if _has_columns(conn, "player_movements", {"canonical_team_id"}):
        checks.append(
            {
                "name": "player_movements require canonical team",
                "base_sql": "FROM player_movements AS t WHERE t.canonical_team_id IS NULL",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
    if _has_columns(conn, "player_movements", {"resolution_status"}):
        checks.append(
            {
                "name": "player_movements unresolved player links",
                "base_sql": (
                    "FROM player_movements AS t WHERE t.resolution_status IN ('unresolved', 'unresolved_player')"
                ),
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
                "severity": "warning",
            }
        )
    if _has_columns(conn, "players", {"kbo_person_id", "player_basic_id"}):
        checks.append(
            {
                "name": "players mirror canonical player_basic_id mismatch",
                "base_sql": (
                    "FROM players AS t JOIN player_basic AS p "
                    "ON CAST(t.kbo_person_id AS INTEGER) = p.player_id "
                    "WHERE t.kbo_person_id <> '' AND t.kbo_person_id NOT GLOB '*[^0-9]*' "
                    "AND (t.player_basic_id IS NULL OR t.player_basic_id <> p.player_id)"
                ),
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
            }
        )
        checks.append(
            {
                "name": "players legacy rows without player_basic mirror",
                "base_sql": "FROM players AS t WHERE t.player_basic_id IS NULL",
                "distinct_expr": "t.id",
                "sample_expr": "t.id",
                "severity": "warning",
            }
        )

    if _has_columns(conn, "player_basic", {"player_id", "name"}):
        checks.append(
            {
                "name": "player_basic Unknown ID stubs",
                "base_sql": f"FROM player_basic AS p WHERE {_unknown_player_predicate('p')}",
                "distinct_expr": "p.player_id",
                "sample_expr": "p.player_id",
            }
        )

    return [
        _run_count_check(
            conn,
            name=check["name"],
            base_sql=check["base_sql"],
            distinct_expr=check["distinct_expr"],
            sample_expr=check["sample_expr"],
            sample_limit=sample_limit,
            severity=check.get("severity", "fail"),
        )
        for check in checks
    ]


def collect_sqlite_report(db_path: Path, sample_limit: int) -> dict[str, Any]:
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        results = [
            _foreign_key_check(conn, sample_limit),
            _integrity_column_report(conn),
            _declared_fk_report(conn),
            _cascade_fk_report(conn),
        ]
        results.extend(_targeted_checks(conn, sample_limit))
        return {
            "database": str(db_path),
            "ok": not any(result.failed for result in results),
            "checks": [asdict(result) for result in results],
        }
    finally:
        conn.close()


def collect_database_url_report(db_url: str, sample_limit: int) -> dict[str, Any]:
    if db_url.startswith("sqlite:///"):
        return collect_sqlite_report(_sqlite_path_from_url(db_url), sample_limit)

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            results = [
                CheckResult(
                    name=f"{conn.dialect.name} native FK check",
                    status="WARN",
                    severity="warning",
                    samples=["native FK scan is not implemented for this dialect; logical checks were executed"],
                ),
                _sa_integrity_column_report(conn),
                _sa_declared_fk_report(conn),
                _sa_cascade_fk_report(conn),
            ]
            results.extend(_sa_targeted_checks(conn, sample_limit))
            return {
                "database": _mask_url(db_url),
                "ok": not any(result.failed for result in results),
                "checks": [asdict(result) for result in results],
            }
    finally:
        engine.dispose()


def collect_report(db_path: Path, sample_limit: int) -> dict[str, Any]:
    return collect_sqlite_report(db_path, sample_limit)


def print_human_report(report: dict[str, Any]) -> None:
    logger.info("=== Orphan Data Verification Report ===")
    logger.info(f"Database: {report['database']}\n")
    for check in report["checks"]:
        status = check["status"]
        logger.info(f"[{status}] {check['name']}: rows={check['row_count']} distinct={check['distinct_count']}")
        if check.get("error"):
            logger.info(f"  error: {check['error']}")
        samples = check.get("samples") or []
        if samples:
            rendered = ", ".join(str(sample) for sample in samples)
            logger.info(f"  samples: {rendered}")
    logger.info(f"\nVerification {'passed' if report['ok'] else 'failed'}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check KBO referential integrity gaps.")
    parser.add_argument("--db-path", help="Path to SQLite DB. Defaults to data/kbo_dev.db.")
    parser.add_argument("--db-url", help="Database URL. Supports sqlite:///, SQLAlchemy URLs, or env:VAR_NAME.")
    parser.add_argument("--sample-limit", type=int, default=5, help="Number of sample IDs to show per failing check.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON only.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when fail-severity checks fail.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    if args.db_path and args.db_url:
        logger.error("Error: Use either --db-path or --db-url, not both")
        raise SystemExit(2)

    try:
        db_url = _resolve_db_url(args.db_url)
    except ValueError as exc:
        logger.error(f"Error: {exc}")
        raise SystemExit(2)

    if db_url:
        report = collect_database_url_report(db_url, args.sample_limit)
    else:
        db_path = Path(args.db_path) if args.db_path else DEFAULT_DB_PATH
        if not db_path.exists():
            logger.error(f"Error: Database not found at {db_path}")
            raise SystemExit(2)
        report = collect_sqlite_report(db_path, args.sample_limit)
    if args.json:
        logger.info(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_human_report(report)

    if args.strict and not report["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
