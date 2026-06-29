#!/usr/bin/env python3
"""Comprehensive, schema-aware data integrity audit for KBO databases."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import json
import os
import sys
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


DEFAULT_DB_URL = "sqlite:///data/kbo_dev.db"


def _configure_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


GATE_METRIC_KEYS = (
    "batting_null_player_id",
    "pitching_null_player_id",
    "lineups_null_player_id",
    "orphaned_batting_stats",
    "orphaned_pitching_stats",
    "orphaned_lineups",
    "game_batting_duplicate_player_groups",
    "game_pitching_duplicate_player_groups",
    "game_lineups_duplicate_player_team_groups",
    "game_batting_player_team_collisions",
    "game_pitching_player_team_collisions",
    "game_lineups_player_team_collisions",
    "batting_hits_gt_at_bats",
    "batting_at_bats_gt_plate_appearances",
    "pitching_earned_runs_gt_runs_allowed",
    "pseudo_player_profiles",
)


_COLUMNS_CACHE: dict[str, set[str]] = {}


def _execute_scalar(conn, sql: str, params: Mapping[str, Any] | None = None) -> int:
    return int(conn.execute(text(sql), dict(params or {})).scalar() or 0)


def _execute_rows(conn, sql: str, params: Mapping[str, Any] | None = None) -> list[Any]:
    return list(conn.execute(text(sql), dict(params or {})).fetchall())


def _dialect_name(conn) -> str:
    dialect = getattr(conn, "dialect", None)
    if dialect is not None:
        return str(dialect.name)
    bind = getattr(conn, "bind", None)
    if bind is None and hasattr(conn, "get_bind"):
        bind = conn.get_bind()
    dialect = getattr(bind, "dialect", None)
    return str(getattr(dialect, "name", ""))


def table_exists(conn, table_name: str) -> bool:
    dialect = _dialect_name(conn)
    if dialect == "sqlite":
        return bool(
            conn.execute(
                text("SELECT 1 FROM sqlite_master WHERE type='table' AND name = :table_name"),
                {"table_name": table_name},
            ).first(),
        )
    return bool(conn.execute(text("SELECT to_regclass(:table_name)"), {"table_name": table_name}).scalar())


def table_columns(conn, table_name: str) -> set[str]:
    if table_name in _COLUMNS_CACHE:
        return _COLUMNS_CACHE[table_name]
    if not table_exists(conn, table_name):
        _COLUMNS_CACHE[table_name] = set()
        return set()
    dialect = _dialect_name(conn)
    if dialect == "sqlite":
        cols = {str(row[1]) for row in conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()}
        _COLUMNS_CACHE[table_name] = cols
        return cols
    rows = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
              AND table_schema = CURRENT_SCHEMA()
            """,
        ),
        {"table_name": table_name},
    ).fetchall()
    cols = {str(row[0]) for row in rows}
    _COLUMNS_CACHE[table_name] = cols
    return cols


def _has_columns(conn, table_name: str, columns: Iterable[str]) -> bool:
    available = table_columns(conn, table_name)
    return all(column in available for column in columns)


def _count_orphans(
    conn,
    *,
    table_name: str,
    column: str,
    parent_table: str,
    parent_column: str,
) -> int:
    if not _has_columns(conn, table_name, (column,)) or not _has_columns(conn, parent_table, (parent_column,)):
        return 0
    return _execute_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {table_name} child
        WHERE child.{column} IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {parent_table} parent
              WHERE parent.{parent_column} = child.{column}
          )
        """,
    )


def _count_duplicate_groups(conn, table_name: str, columns: tuple[str, ...], *, where: str = "") -> int:
    if not _has_columns(conn, table_name, columns):
        return 0
    cols = ", ".join(columns)
    where_sql = f"WHERE {where}" if where else ""
    return _execute_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {cols}, COUNT(*) AS row_count
            FROM {table_name}
            {where_sql}
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        ) grouped
        """,
    )


def _count_player_team_collisions(conn, table_name: str) -> int:
    if not _has_columns(conn, table_name, ("game_id", "player_id", "team_side", "team_code")):
        return 0
    return _execute_scalar(
        conn,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT game_id, player_id,
                   COUNT(DISTINCT COALESCE(team_side, '') || ':' || COALESCE(team_code, '')) AS team_count
            FROM {table_name}
            WHERE player_id IS NOT NULL
            GROUP BY game_id, player_id
            HAVING COUNT(DISTINCT COALESCE(team_side, '') || ':' || COALESCE(team_code, '')) > 1
        ) grouped
        """,
    )


def _count_condition(conn, table_name: str, required_columns: tuple[str, ...], condition: str) -> int:
    if not _has_columns(conn, table_name, required_columns):
        return 0
    return _execute_scalar(conn, f"SELECT COUNT(*) FROM {table_name} WHERE {condition}")


def _distribution(
    conn,
    table_name: str,
    group_column: str,
    condition_columns: tuple[str, ...],
    condition: str,
) -> dict[str, int]:
    required = (group_column, *condition_columns)
    if not _has_columns(conn, table_name, required):
        return {}
    rows = _execute_rows(
        conn,
        f"""
        SELECT COALESCE(CAST({group_column} AS TEXT), '') AS bucket, COUNT(*) AS count
        FROM {table_name}
        WHERE {condition}
        GROUP BY {group_column}
        ORDER BY count DESC, bucket
        """,
    )
    return {str(row[0]): int(row[1] or 0) for row in rows}


def _valid_team_code_select(conn) -> str | None:
    columns = table_columns(conn, "teams")
    candidates = [column for column in ("team_id", "code") if column in columns]
    if "alternate_code" in columns:
        candidates.append("alternate_code")
    if not candidates:
        return None
    parts = [f"SELECT {column} AS team_code FROM teams WHERE {column} IS NOT NULL" for column in candidates]
    return " UNION ".join(parts)


def collect_audit_metrics(conn) -> dict[str, Any]:
    """Collect nested audit metrics without assuming every optional column exists."""
    orphans = {
        "game_batting_stats.game_id": _count_orphans(
            conn,
            table_name="game_batting_stats",
            column="game_id",
            parent_table="game",
            parent_column="game_id",
        ),
        "game_pitching_stats.game_id": _count_orphans(
            conn,
            table_name="game_pitching_stats",
            column="game_id",
            parent_table="game",
            parent_column="game_id",
        ),
        "game_lineups.game_id": _count_orphans(
            conn,
            table_name="game_lineups",
            column="game_id",
            parent_table="game",
            parent_column="game_id",
        ),
        "player_season_batting.player_id": _count_orphans(
            conn,
            table_name="player_season_batting",
            column="player_id",
            parent_table="player_basic",
            parent_column="player_id",
        ),
        "player_season_pitching.player_id": _count_orphans(
            conn,
            table_name="player_season_pitching",
            column="player_id",
            parent_table="player_basic",
            parent_column="player_id",
        ),
        "game_batting_stats.player_id": _count_orphans(
            conn,
            table_name="game_batting_stats",
            column="player_id",
            parent_table="player_basic",
            parent_column="player_id",
        ),
        "game_pitching_stats.player_id": _count_orphans(
            conn,
            table_name="game_pitching_stats",
            column="player_id",
            parent_table="player_basic",
            parent_column="player_id",
        ),
        "game_lineups.player_id": _count_orphans(
            conn,
            table_name="game_lineups",
            column="player_id",
            parent_table="player_basic",
            parent_column="player_id",
        ),
    }

    duplicates = {
        "player_season_batting_identity_groups": _count_duplicate_groups(
            conn,
            "player_season_batting",
            ("player_id", "season", "league", "level", "team_code", "source"),
            where="player_id IS NOT NULL",
        ),
        "player_season_pitching_identity_groups": _count_duplicate_groups(
            conn,
            "player_season_pitching",
            ("player_id", "season", "league", "level", "team_code", "source"),
            where="player_id IS NOT NULL",
        ),
        "game_batting_duplicate_player_groups": _count_duplicate_groups(
            conn,
            "game_batting_stats",
            ("game_id", "player_id"),
            where="player_id IS NOT NULL",
        ),
        "game_pitching_duplicate_player_groups": _count_duplicate_groups(
            conn,
            "game_pitching_stats",
            ("game_id", "player_id"),
            where="player_id IS NOT NULL",
        ),
        "game_lineups_duplicate_player_team_groups": _count_duplicate_groups(
            conn,
            "game_lineups",
            ("game_id", "player_id", "team_code"),
            where="player_id IS NOT NULL",
        ),
        "player_basic_duplicate_player_id_groups": _count_duplicate_groups(
            conn,
            "player_basic",
            ("player_id",),
        ),
        "game_duplicate_game_id_groups": _count_duplicate_groups(
            conn,
            "game",
            ("game_id",),
        ),
    }

    team_collisions = {
        "game_batting_player_team_collisions": _count_player_team_collisions(conn, "game_batting_stats"),
        "game_pitching_player_team_collisions": _count_player_team_collisions(conn, "game_pitching_stats"),
        "game_lineups_player_team_collisions": _count_player_team_collisions(conn, "game_lineups"),
    }

    logical_errors = {
        "batting_hits_gt_at_bats": _count_condition(
            conn,
            "player_season_batting",
            ("hits", "at_bats"),
            "hits IS NOT NULL AND at_bats IS NOT NULL AND hits > at_bats",
        ),
        "batting_at_bats_gt_plate_appearances": _count_condition(
            conn,
            "player_season_batting",
            ("at_bats", "plate_appearances"),
            "at_bats IS NOT NULL AND plate_appearances IS NOT NULL AND at_bats > plate_appearances",
        ),
        "pitching_earned_runs_gt_runs_allowed": _count_condition(
            conn,
            "player_season_pitching",
            ("earned_runs", "runs_allowed"),
            "earned_runs IS NOT NULL AND runs_allowed IS NOT NULL AND earned_runs > runs_allowed",
        ),
        "future_final_games": _count_condition(
            conn,
            "game",
            ("game_date", "game_status"),
            "game_date > CURRENT_DATE AND game_status IN ('FINAL', 'COMPLETED', 'DRAW')",
        ),
    }

    nulls = {
        "player_basic.name": _count_condition(conn, "player_basic", ("name",), "name IS NULL"),
        "player_season_batting.team_code": _count_condition(
            conn,
            "player_season_batting",
            ("team_code",),
            "team_code IS NULL",
        ),
        "game.game_date": _count_condition(conn, "game", ("game_date",), "game_date IS NULL"),
        "game.home_score_final": _count_condition(
            conn,
            "game",
            ("home_score", "game_status"),
            "home_score IS NULL AND game_status IN ('FINAL', 'COMPLETED', 'DRAW')",
        ),
        "game.away_score_final": _count_condition(
            conn,
            "game",
            ("away_score", "game_status"),
            "away_score IS NULL AND game_status IN ('FINAL', 'COMPLETED', 'DRAW')",
        ),
        "game_batting_stats.player_id": _count_condition(
            conn,
            "game_batting_stats",
            ("player_id",),
            "player_id IS NULL",
        ),
        "game_pitching_stats.player_id": _count_condition(
            conn,
            "game_pitching_stats",
            ("player_id",),
            "player_id IS NULL",
        ),
        "game_lineups.player_id": _count_condition(
            conn,
            "game_lineups",
            ("player_id",),
            "player_id IS NULL",
        ),
    }

    valid_team_select = _valid_team_code_select(conn)
    invalid_batting_team_codes = 0
    if valid_team_select and _has_columns(conn, "player_season_batting", ("team_code",)):
        invalid_batting_team_codes = _execute_scalar(
            conn,
            f"""
            SELECT COUNT(DISTINCT team_code)
            FROM player_season_batting
            WHERE team_code IS NOT NULL
              AND team_code NOT IN ({valid_team_select})
            """,
        )

    pseudo_profiles = {
        "player_basic_ge_900000": _count_condition(
            conn,
            "player_basic",
            ("player_id",),
            "player_id >= 900000",
        ),
        "player_basic_ge_900000_missing_team": _count_condition(
            conn,
            "player_basic",
            ("player_id", "team"),
            "player_id >= 900000 AND (team IS NULL OR TRIM(team) = '')",
        ),
    }

    identity_missing = 0
    if _has_columns(conn, "player_basic", ("player_id",)) and _has_columns(conn, "players", ("id", "kbo_person_id")):
        identity_missing = _execute_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM player_basic pb
            WHERE NOT EXISTS (
                SELECT 1
                FROM players p
                WHERE CAST(p.kbo_person_id AS TEXT) = CAST(pb.player_id AS TEXT)
            )
            """,
        )

    distributions = {
        "batting_at_bats_gt_plate_appearances_by_source": _distribution(
            conn,
            "player_season_batting",
            "source",
            ("at_bats", "plate_appearances"),
            "at_bats IS NOT NULL AND plate_appearances IS NOT NULL AND at_bats > plate_appearances",
        ),
        "batting_at_bats_gt_plate_appearances_by_year": _distribution(
            conn,
            "player_season_batting",
            "season",
            ("at_bats", "plate_appearances"),
            "at_bats IS NOT NULL AND plate_appearances IS NOT NULL AND at_bats > plate_appearances",
        ),
        "pitching_earned_runs_gt_runs_allowed_by_source": _distribution(
            conn,
            "player_season_pitching",
            "source",
            ("earned_runs", "runs_allowed"),
            "earned_runs IS NOT NULL AND runs_allowed IS NOT NULL AND earned_runs > runs_allowed",
        ),
        "pitching_earned_runs_gt_runs_allowed_by_year": _distribution(
            conn,
            "player_season_pitching",
            "season",
            ("earned_runs", "runs_allowed"),
            "earned_runs IS NOT NULL AND runs_allowed IS NOT NULL AND earned_runs > runs_allowed",
        ),
    }

    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat() + "Z",
        "orphans": orphans,
        "duplicates": duplicates,
        "team_collisions": team_collisions,
        "logical_errors": logical_errors,
        "nulls": nulls,
        "team_code_consistency": {
            "player_season_batting_invalid_team_codes": invalid_batting_team_codes,
        },
        "player_identity": {
            "player_basic_missing_players_kbo_person_map": identity_missing,
        },
        "pseudo_profiles": pseudo_profiles,
        "distributions": distributions,
    }


def flatten_gate_metrics(report: Mapping[str, Any]) -> dict[str, int]:
    """Flatten audit metrics that should be enforced by quality_gate baselines."""
    orphans = report.get("orphans", {})
    duplicates = report.get("duplicates", {})
    collisions = report.get("team_collisions", {})
    logical = report.get("logical_errors", {})
    nulls = report.get("nulls", {})
    pseudo = report.get("pseudo_profiles", {})
    flat = {
        "batting_null_player_id": int(nulls.get("game_batting_stats.player_id", 0) or 0),
        "pitching_null_player_id": int(nulls.get("game_pitching_stats.player_id", 0) or 0),
        "lineups_null_player_id": int(nulls.get("game_lineups.player_id", 0) or 0),
        "orphaned_batting_stats": int(orphans.get("game_batting_stats.game_id", 0) or 0),
        "orphaned_pitching_stats": int(orphans.get("game_pitching_stats.game_id", 0) or 0),
        "orphaned_lineups": int(orphans.get("game_lineups.game_id", 0) or 0),
        "game_batting_duplicate_player_groups": int(duplicates.get("game_batting_duplicate_player_groups", 0) or 0),
        "game_pitching_duplicate_player_groups": int(duplicates.get("game_pitching_duplicate_player_groups", 0) or 0),
        "game_lineups_duplicate_player_team_groups": int(
            duplicates.get("game_lineups_duplicate_player_team_groups", 0) or 0,
        ),
        "game_batting_player_team_collisions": int(collisions.get("game_batting_player_team_collisions", 0) or 0),
        "game_pitching_player_team_collisions": int(collisions.get("game_pitching_player_team_collisions", 0) or 0),
        "game_lineups_player_team_collisions": int(collisions.get("game_lineups_player_team_collisions", 0) or 0),
        "batting_hits_gt_at_bats": int(logical.get("batting_hits_gt_at_bats", 0) or 0),
        "batting_at_bats_gt_plate_appearances": int(logical.get("batting_at_bats_gt_plate_appearances", 0) or 0),
        "pitching_earned_runs_gt_runs_allowed": int(logical.get("pitching_earned_runs_gt_runs_allowed", 0) or 0),
        "pseudo_player_profiles": int(pseudo.get("player_basic_ge_900000", 0) or 0),
    }
    return {key: int(flat.get(key, 0)) for key in GATE_METRIC_KEYS}


def evaluate_strict_zero(report: Mapping[str, Any]) -> list[str]:
    failures = []
    for key, value in flatten_gate_metrics(report).items():
        if int(value) > 0:
            failures.append(f"{key}={value} must be 0")
    return failures


def _print_section(title: str, values: Mapping[str, Any]) -> None:
    logger.info(f"\n--- {title} ---")
    for key, value in values.items():
        logger.info(f"  {key}: {value}")


def print_human_report(report: Mapping[str, Any], strict_failures: list[str]) -> None:
    logger.info("=== KBO Database Comprehensive Audit ===")
    _print_section("1. Orphaned Records", report["orphans"])
    _print_section("2. Duplicate Records", report["duplicates"])
    _print_section("3. Player Team Collisions", report["team_collisions"])
    _print_section("4. Logical Errors", report["logical_errors"])
    _print_section("5. NULLs in Key Fields", report["nulls"])
    _print_section("6. Team Code Consistency", report["team_code_consistency"])
    _print_section("7. Player Identity", report["player_identity"])
    _print_section("8. Pseudo Profiles", report["pseudo_profiles"])

    distributions = report.get("distributions", {})
    logger.error("\n--- 9. Logical Error Distributions ---")
    for key, values in distributions.items():
        preview = dict(list(values.items())[:10])
        logger.info(f"  {key}: {preview}")

    if strict_failures:
        logger.info("\n--- Strict Zero Failures ---")
        for failure in strict_failures:
            logger.info(f"  - {failure}")


def run_audit(
    *,
    db_url: str = DEFAULT_DB_URL,
    strict_zero: bool = False,
    write_artifacts: bool = False,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        report = collect_audit_metrics(conn)
    strict_failures = evaluate_strict_zero(report) if strict_zero else []
    result = {
        "ok": len(strict_failures) == 0,
        "strict_zero": strict_zero,
        "strict_failures": strict_failures,
        "gate_metrics": flatten_gate_metrics(report),
        "report": report,
        "artifact_path": None,
    }

    if write_artifacts:
        target_dir = output_dir or (PROJECT_ROOT / "data")
        target_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        artifact_path = target_dir / f"full_audit_{stamp}.json"
        artifact_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        result["artifact_path"] = str(artifact_path)

    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run comprehensive KBO database integrity audit")
    parser.add_argument("--db-url", default=None, help="SQLAlchemy database URL. Defaults to DATABASE_URL/local DB.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--strict-zero", action="store_true", help="Fail if any gate metric is non-zero.")
    parser.add_argument("--output-dir", default="data", help="Directory for optional JSON audit artifact.")
    parser.add_argument(
        "--no-write",
        "--no-artifacts",
        dest="write_artifacts",
        action="store_false",
        help="Do not write a JSON artifact.",
    )
    parser.add_argument(
        "--write",
        "--write-artifacts",
        dest="write_artifacts",
        action="store_true",
        help="Write a JSON artifact.",
    )
    parser.set_defaults(write_artifacts=False)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    _configure_logging()
    load_dotenv()
    args = parse_args(argv)
    db_url = args.db_url or os.getenv("DATABASE_URL") or DEFAULT_DB_URL
    result = run_audit(
        db_url=db_url,
        strict_zero=args.strict_zero,
        write_artifacts=bool(args.write_artifacts),
        output_dir=Path(args.output_dir),
    )

    if args.json:
        logger.info(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print_human_report(result["report"], result["strict_failures"])
        if result["artifact_path"]:
            logger.info(f"\nArtifact: {result['artifact_path']}")
        logger.info(f"\nStatus: {'PASS' if result['ok'] else 'FAIL'}")

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
