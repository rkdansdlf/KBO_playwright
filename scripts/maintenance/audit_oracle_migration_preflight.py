"""Run a read-only Oracle preflight for the high-risk 024 migration.

The preflight does not create tables, disable triggers, alter constraints, or
write a migration marker. It records the source objects, planned additions,
foreign-key orphan counts, identity-resolution risks, and current trigger
states needed before an Oracle implementation can be reviewed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import create_engine_for_url

if TYPE_CHECKING:
    from collections.abc import Sequence
    from sqlalchemy.engine import Connection

MIGRATION_NAME = "024_deletion_anomaly_integrity"

REQUIRED_COLUMNS: dict[str, set[str]] = {
    "PLAYERS": {"KBO_PERSON_ID"},
    "TEAM_DAILY_ROSTER": {"PLAYER_ID", "POSITION"},
    "PLAYER_MOVEMENTS": {"ID", "TEAM_CODE", "MOVEMENT_DATE", "PLAYER_NAME"},
    "PLAYER_BASIC": {"PLAYER_ID", "NAME", "TEAM"},
    "PLAYER_SEASON_BATTING": {"PLAYER_ID", "TEAM_CODE", "SEASON"},
    "PLAYER_SEASON_PITCHING": {"PLAYER_ID", "TEAM_CODE", "SEASON"},
    "TEAMS": {"TEAM_ID", "TEAM_SHORT_NAME", "TEAM_NAME"},
}

PLANNED_COLUMNS: dict[str, set[str]] = {
    "PLAYERS": {"PLAYER_BASIC_ID"},
    "TEAM_DAILY_ROSTER": {"PLAYER_BASIC_ID", "PERSON_TYPE"},
    "PLAYER_MOVEMENTS": {"CANONICAL_TEAM_ID", "PLAYER_BASIC_ID", "RESOLUTION_STATUS"},
}

PLANNED_FOREIGN_KEYS: tuple[tuple[str, str, str, str], ...] = (
    ("PLAYERS", "PLAYER_BASIC_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("TEAM_DAILY_ROSTER", "TEAM_CODE", "TEAMS", "TEAM_ID"),
    ("TEAM_DAILY_ROSTER", "PLAYER_BASIC_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("PLAYER_MOVEMENTS", "CANONICAL_TEAM_ID", "TEAMS", "TEAM_ID"),
    ("PLAYER_MOVEMENTS", "PLAYER_BASIC_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("GAME_ID_ALIASES", "CANONICAL_GAME_ID", "GAME", "GAME_ID"),
    ("GAME_METADATA", "GAME_ID", "GAME", "GAME_ID"),
    ("GAME_INNING_SCORES", "GAME_ID", "GAME", "GAME_ID"),
    ("GAME_LINEUPS", "GAME_ID", "GAME", "GAME_ID"),
    ("GAME_LINEUPS", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("GAME_BATTING_STATS", "GAME_ID", "GAME", "GAME_ID"),
    ("GAME_BATTING_STATS", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("GAME_PITCHING_STATS", "GAME_ID", "GAME", "GAME_ID"),
    ("GAME_PITCHING_STATS", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("GAME_EVENTS", "GAME_ID", "GAME", "GAME_ID"),
    ("GAME_EVENTS", "BATTER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("GAME_EVENTS", "PITCHER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("GAME_SUMMARY", "GAME_ID", "GAME", "GAME_ID"),
    ("GAME_SUMMARY", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("GAME_PLAY_BY_PLAY", "GAME_ID", "GAME", "GAME_ID"),
    ("MATCHUP_BVP", "BATTER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("MATCHUP_BVP", "PITCHER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("MATCHUP_BATTER_SPLITS", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("MATCHUP_PITCHER_SPLITS", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("MATCHUP_BATTER_TEAM_SPLIT", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("MATCHUP_PITCHER_TEAM_SPLIT", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("MATCHUP_BATTER_STADIUM_SPLIT", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
    ("MATCHUP_BATTER_VS_STARTER", "PLAYER_ID", "PLAYER_BASIC", "PLAYER_ID"),
)

TEAM_CODE_EXPRESSION = """
CASE UPPER(TRIM(pm.team_code))
    WHEN 'KIA' THEN 'KIA'
    WHEN '기아' THEN 'KIA'
    WHEN '두산' THEN 'DB'
    WHEN 'DB' THEN 'DB'
    WHEN 'OB' THEN 'OB'
    WHEN '롯데' THEN 'LT'
    WHEN 'LT' THEN 'LT'
    WHEN '삼성' THEN 'SS'
    WHEN 'SS' THEN 'SS'
    WHEN '한화' THEN 'HH'
    WHEN 'HH' THEN 'HH'
    WHEN '키움' THEN 'KH'
    WHEN 'KH' THEN 'KH'
    WHEN '넥센' THEN 'NX'
    WHEN 'NX' THEN 'NX'
    WHEN '우리' THEN 'WO'
    WHEN 'WO' THEN 'WO'
    WHEN 'SSG' THEN 'SSG'
    WHEN 'SK' THEN 'SK'
    WHEN 'LG' THEN 'LG'
    WHEN 'KT' THEN 'KT'
    WHEN 'NC' THEN 'NC'
    WHEN '현대' THEN 'HU'
    WHEN 'HU' THEN 'HU'
    WHEN 'HD' THEN 'HU'
    WHEN '해태' THEN 'HT'
    WHEN 'HT' THEN 'HT'
    WHEN '쌍방울' THEN 'SL'
    WHEN 'SL' THEN 'SL'
    WHEN '태평양' THEN 'TP'
    WHEN 'TP' THEN 'TP'
    WHEN '청보' THEN 'CB'
    WHEN 'CB' THEN 'CB'
    WHEN '삼미' THEN 'SM'
    WHEN 'SM' THEN 'SM'
    WHEN '빙그레' THEN 'BE'
    WHEN 'BE' THEN 'BE'
    WHEN 'MBC' THEN 'MBC'
    ELSE TRIM(pm.team_code)
END
""".strip()


def _check(name: str, status: str, detail: str, count: int | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"name": name, "status": status, "detail": detail}
    if count is not None:
        result["count"] = count
    return result


def _user_columns(conn: Connection) -> set[tuple[str, str]]:
    fk_tables = {
        table for child_table, _, parent_table, _ in PLANNED_FOREIGN_KEYS for table in (child_table, parent_table)
    }
    tables = sorted(set(REQUIRED_COLUMNS) | set(PLANNED_COLUMNS) | fk_tables)
    table_values = ", ".join(f"'{table}'" for table in tables)
    rows = conn.execute(
        text(f"SELECT table_name, column_name FROM user_tab_columns WHERE table_name IN ({table_values})"),
    ).fetchall()
    return {(str(row[0]).upper(), str(row[1]).upper()) for row in rows}


def _has_column(columns: set[tuple[str, str]], table: str, column: str) -> bool:
    return (table, column) in columns


def _object_checks(columns: set[tuple[str, str]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for table, required in REQUIRED_COLUMNS.items():
        present = {column for current_table, column in columns if current_table == table}
        missing = sorted(required - present)
        if not present:
            checks.append(
                _check(
                    f"required_object:{table}",
                    "BLOCK",
                    "required source table is not present",
                ),
            )
        elif missing:
            checks.append(
                _check(
                    f"required_object:{table}",
                    "BLOCK",
                    f"missing required source columns: {', '.join(missing)}",
                ),
            )
        else:
            checks.append(_check(f"required_object:{table}", "PASS", "required source columns present"))

    for table, planned in PLANNED_COLUMNS.items():
        for column in sorted(planned):
            status = "PRESENT" if _has_column(columns, table, column) else "PLAN"
            detail = "already present" if status == "PRESENT" else "column will be added by migration"
            checks.append(_check(f"planned_column:{table}.{column}", status, detail))
    return checks


def _scalar_count(conn: Connection, query: str) -> tuple[int | None, str | None]:
    try:
        value = conn.execute(text(query)).scalar()
        return int(value or 0), None
    except SQLAlchemyError as exc:
        return None, f"read-only query failed: {exc}"


def _count_check(
    conn: Connection,
    name: str,
    query: str,
    detail_when_clean: str,
    blocking: bool = True,
) -> dict[str, Any]:
    count, error = _scalar_count(conn, query)
    if error:
        return _check(name, "BLOCK", error)
    assert count is not None
    if count == 0:
        return _check(name, "PASS", detail_when_clean, count)
    status = "BLOCK" if blocking else "WARN"
    return _check(name, status, f"found {count} row(s) requiring review", count)


def _orphan_checks(conn: Connection, columns: set[tuple[str, str]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for child_table, child_column, parent_table, parent_column in PLANNED_FOREIGN_KEYS:
        object_columns = {
            (child_table, child_column),
            (parent_table, parent_column),
        }
        name = f"orphan:{child_table}.{child_column}->{parent_table}.{parent_column}"
        if not object_columns <= columns:
            checks.append(_check(name, "SKIP", "child or parent column is not present in the current schema"))
            continue
        query = f"""
            SELECT COUNT(*)
            FROM {child_table} child_row
            WHERE child_row.{child_column} IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM {parent_table} parent_row
                  WHERE parent_row.{parent_column} = child_row.{child_column}
              )
        """
        checks.append(
            _count_check(
                conn,
                name,
                query,
                "no orphan values found",
            ),
        )
    return checks


def _identity_checks(conn: Connection, columns: set[tuple[str, str]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    if _has_column(columns, "PLAYERS", "PLAYER_BASIC_ID"):
        checks.append(
            _count_check(
                conn,
                "duplicate:PLAYERS.PLAYER_BASIC_ID",
                """
                    SELECT COUNT(*)
                    FROM (
                        SELECT player_basic_id
                        FROM players
                        WHERE player_basic_id IS NOT NULL
                        GROUP BY player_basic_id
                        HAVING COUNT(*) > 1
                    )
                """,
                "no duplicate player_basic_id values found",
            ),
        )

    if {("PLAYERS", "KBO_PERSON_ID"), ("PLAYER_BASIC", "PLAYER_ID")} <= columns:
        checks.append(
            _count_check(
                conn,
                "duplicate:predicted_PLAYERS.PLAYER_BASIC_ID",
                """
                    SELECT COUNT(*)
                    FROM (
                        SELECT TRIM(p.kbo_person_id)
                        FROM players p
                        JOIN player_basic pb
                          ON pb.player_id = CASE
                              WHEN REGEXP_LIKE(TRIM(p.kbo_person_id), '^[0-9]+$')
                              THEN TO_NUMBER(TRIM(p.kbo_person_id))
                          END
                        WHERE REGEXP_LIKE(TRIM(p.kbo_person_id), '^[0-9]+$')
                        GROUP BY TRIM(p.kbo_person_id)
                        HAVING COUNT(*) > 1
                    )
                """,
                "no duplicate player IDs are predicted by the backfill",
            ),
        )
    return checks


def _movement_checks(conn: Connection, columns: set[tuple[str, str]]) -> list[dict[str, Any]]:
    if not {("PLAYER_MOVEMENTS", "TEAM_CODE"), ("TEAMS", "TEAM_ID")} <= columns:
        return [_check("unresolved:PLAYER_MOVEMENTS.CANONICAL_TEAM_ID", "SKIP", "source team columns are not present")]
    query = f"""
        SELECT COUNT(*)
        FROM player_movements pm
        WHERE pm.team_code IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM teams t
              WHERE t.team_id = {TEAM_CODE_EXPRESSION}
          )
    """
    return [
        _count_check(
            conn,
            "unresolved:PLAYER_MOVEMENTS.CANONICAL_TEAM_ID",
            query,
            "all source team codes resolve through the migration mapping",
            blocking=False,
        ),
    ]


def _trigger_states(conn: Connection) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    try:
        rows = conn.execute(
            text(
                """
                SELECT trigger_name, table_name, status, triggering_event, trigger_type
                FROM user_triggers
                WHERE table_name IN ('PLAYERS', 'TEAM_DAILY_ROSTER', 'PLAYER_MOVEMENTS')
                ORDER BY table_name, trigger_name
                """
            ),
        ).fetchall()
    except SQLAlchemyError as exc:
        return [], _check("trigger_states", "BLOCK", f"read-only trigger census failed: {exc}")

    states = [
        {
            "trigger_name": str(row[0]),
            "table_name": str(row[1]),
            "status": str(row[2]),
            "triggering_event": str(row[3]),
            "trigger_type": str(row[4]),
        }
        for row in rows
    ]
    disabled = [state["trigger_name"] for state in states if state["status"].upper() != "ENABLED"]
    detail = "all discovered triggers are enabled" if not disabled else f"disabled triggers: {', '.join(disabled)}"
    return states, _check("trigger_states", "WARN" if disabled else "PASS", detail)


def run_preflight(conn: Connection) -> dict[str, Any]:
    """Run the read-only 024 preflight against an open Oracle connection."""
    columns = _user_columns(conn)
    checks = _object_checks(columns)
    checks.extend(_identity_checks(conn, columns))
    checks.extend(_movement_checks(conn, columns))
    checks.extend(_orphan_checks(conn, columns))
    trigger_states, trigger_check = _trigger_states(conn)
    if trigger_check is not None:
        checks.append(trigger_check)

    blocking = [check["name"] for check in checks if check["status"] == "BLOCK"]
    return {
        "migration": MIGRATION_NAME,
        "read_only": True,
        "preflight_clear": not blocking,
        "blocking_checks": blocking,
        "checks": checks,
        "trigger_states": trigger_states,
    }


def render_text(report: dict[str, Any]) -> str:
    lines = [
        f"Oracle preflight: {report['migration']}",
        f"Read-only: {report['read_only']}",
        f"Preflight clear: {report['preflight_clear']}",
    ]
    for check in report["checks"]:
        count = f" count={check['count']}" if "count" in check else ""
        lines.append(f"[{check['status']}] {check['name']}{count}: {check['detail']}")
    return "\n".join(lines)


def _write_report(report: dict[str, Any], output: Path | None, as_json: bool) -> None:
    rendered = json.dumps(report, ensure_ascii=False, indent=2) if as_json else render_text(report)
    sys.stdout.write(rendered + "\n")
    if output is not None:
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Oracle preflight CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Render the report as JSON")
    parser.add_argument("--output", type=Path, help="Write a JSON report to this path")
    args = parser.parse_args(argv)

    load_dotenv()
    url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not url:
        sys.stderr.write("OCI_DB_URL or TARGET_DATABASE_URL is not set.\n")
        return 2

    engine = create_engine_for_url(url)
    try:
        with engine.connect() as conn:
            report = run_preflight(conn)
    except SQLAlchemyError as exc:
        sys.stderr.write(f"Oracle preflight connection/query failed: {exc}\n")
        return 2
    finally:
        engine.dispose()

    _write_report(report, args.output, args.json)
    return 0 if report["preflight_clear"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
