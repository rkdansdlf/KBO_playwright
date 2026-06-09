#!/usr/bin/env python3
"""Export detailed worklists for known data integrity debt.

This tool is intentionally read-only. It creates CSVs that separate automated
repair blockers from rows that need source recrawl or manual identity review.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL  # noqa: E402

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "integrity_worklists"


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})
            count += 1
    return count


def _rows(conn, sql: str) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(text(sql)).mappings().all()]


def _export_null_player_ids(conn, output_dir: Path, stamp: str) -> tuple[str, int]:
    sql = """
        SELECT 'game_batting_stats' AS table_name, id, game_id, team_side, team_code,
               player_name, uniform_no, appearance_seq, batting_order
        FROM game_batting_stats
        WHERE player_id IS NULL
        UNION ALL
        SELECT 'game_pitching_stats' AS table_name, id, game_id, team_side, team_code,
               player_name, uniform_no, appearance_seq, NULL AS batting_order
        FROM game_pitching_stats
        WHERE player_id IS NULL
        UNION ALL
        SELECT 'game_lineups' AS table_name, id, game_id, team_side, team_code,
               player_name, uniform_no, appearance_seq, batting_order
        FROM game_lineups
        WHERE player_id IS NULL
        ORDER BY table_name, game_id, team_side, appearance_seq, player_name
    """
    path = output_dir / f"null_player_ids_{stamp}.csv"
    count = _write_csv(
        path,
        _rows(conn, sql),
        [
            "table_name",
            "id",
            "game_id",
            "team_side",
            "team_code",
            "player_name",
            "uniform_no",
            "appearance_seq",
            "batting_order",
        ],
    )
    return str(path), count


def _export_duplicate_groups(
    conn, output_dir: Path, stamp: str, table_name: str, group_columns: str
) -> tuple[str, int]:
    sql = f"""
        WITH duplicate_groups AS (
            SELECT {group_columns}, COUNT(*) AS row_count
            FROM {table_name}
            WHERE player_id IS NOT NULL
            GROUP BY {group_columns}
            HAVING COUNT(*) > 1
        )
        SELECT '{table_name}' AS table_name, t.*
        FROM {table_name} t
        JOIN duplicate_groups d
          ON {group_columns_join("t", "d", group_columns)}
        ORDER BY t.game_id, t.player_id, t.id
    """
    path = output_dir / f"{table_name}_duplicate_rows_{stamp}.csv"
    rows = _rows(conn, sql)
    fieldnames = sorted({key for row in rows for key in row})
    if not fieldnames:
        fieldnames = ["table_name"]
    count = _write_csv(path, rows, fieldnames)
    return str(path), count


def group_columns_join(left_alias: str, right_alias: str, group_columns: str) -> str:
    columns = [column.strip() for column in group_columns.split(",")]
    return " AND ".join(f"{left_alias}.{column} = {right_alias}.{column}" for column in columns)


def _export_team_collisions(conn, output_dir: Path, stamp: str, table_name: str) -> tuple[str, int]:
    sql = f"""
        WITH collisions AS (
            SELECT game_id, player_id
            FROM {table_name}
            WHERE player_id IS NOT NULL
            GROUP BY game_id, player_id
            HAVING COUNT(DISTINCT COALESCE(team_side, '') || ':' || COALESCE(team_code, '')) > 1
        )
        SELECT '{table_name}' AS table_name, t.*
        FROM {table_name} t
        JOIN collisions c
          ON c.game_id = t.game_id
         AND c.player_id = t.player_id
        ORDER BY t.game_id, t.player_id, t.team_side, t.id
    """
    path = output_dir / f"{table_name}_team_collisions_{stamp}.csv"
    rows = _rows(conn, sql)
    fieldnames = sorted({key for row in rows for key in row})
    if not fieldnames:
        fieldnames = ["table_name"]
    count = _write_csv(path, rows, fieldnames)
    return str(path), count


def _export_impossible_batting(conn, output_dir: Path, stamp: str) -> tuple[str, int]:
    sql = """
        SELECT psb.id, psb.player_id, pb.name AS player_name, psb.season, psb.league,
               psb.level, psb.source, psb.team_code, psb.games,
               psb.hits, psb.at_bats, psb.plate_appearances
        FROM player_season_batting psb
        LEFT JOIN player_basic pb ON pb.player_id = psb.player_id
        WHERE (psb.hits IS NOT NULL AND psb.at_bats IS NOT NULL AND psb.hits > psb.at_bats)
           OR (psb.at_bats IS NOT NULL AND psb.plate_appearances IS NOT NULL AND psb.at_bats > psb.plate_appearances)
        ORDER BY psb.season, psb.source, psb.player_id
    """
    path = output_dir / f"player_season_batting_impossible_stats_{stamp}.csv"
    count = _write_csv(
        path,
        _rows(conn, sql),
        [
            "id",
            "player_id",
            "player_name",
            "season",
            "league",
            "level",
            "source",
            "team_code",
            "games",
            "hits",
            "at_bats",
            "plate_appearances",
        ],
    )
    return str(path), count


def _export_impossible_pitching(conn, output_dir: Path, stamp: str) -> tuple[str, int]:
    sql = """
        SELECT psp.id, psp.player_id, pb.name AS player_name, psp.season, psp.league,
               psp.level, psp.source, psp.team_code, psp.games,
               psp.earned_runs, psp.runs_allowed
        FROM player_season_pitching psp
        LEFT JOIN player_basic pb ON pb.player_id = psp.player_id
        WHERE psp.earned_runs IS NOT NULL
          AND psp.runs_allowed IS NOT NULL
          AND psp.earned_runs > psp.runs_allowed
        ORDER BY psp.season, psp.source, psp.player_id
    """
    path = output_dir / f"player_season_pitching_impossible_stats_{stamp}.csv"
    count = _write_csv(
        path,
        _rows(conn, sql),
        [
            "id",
            "player_id",
            "player_name",
            "season",
            "league",
            "level",
            "source",
            "team_code",
            "games",
            "earned_runs",
            "runs_allowed",
        ],
    )
    return str(path), count


def _export_unresolved_player_movements(conn, output_dir: Path, stamp: str) -> tuple[str, int]:
    sql = """
        SELECT
            pm.id,
            pm.movement_date,
            pm.section,
            pm.team_code,
            pm.canonical_team_id,
            pm.player_name,
            regexp_replace(COALESCE(pm.player_name, ''), '\\s*\\([^)]*\\)\\s*$', '') AS normalized_player_name,
            pm.resolution_status,
            pm.remarks
        FROM player_movements pm
        WHERE pm.resolution_status IN ('unresolved', 'unresolved_player')
        ORDER BY pm.movement_date, pm.canonical_team_id, pm.player_name, pm.id
    """
    path = output_dir / f"player_movements_unresolved_players_{stamp}.csv"
    count = _write_csv(
        path,
        _rows(conn, sql),
        [
            "id",
            "movement_date",
            "section",
            "team_code",
            "canonical_team_id",
            "player_name",
            "normalized_player_name",
            "resolution_status",
            "remarks",
        ],
    )
    return str(path), count


def _export_players_without_mirror(conn, output_dir: Path, stamp: str) -> tuple[str, int]:
    sql = """
        SELECT
            p.id,
            p.kbo_person_id,
            pi.name_kor AS primary_name_kor,
            pi.name_eng AS primary_name_eng,
            p.status,
            p.birth_date,
            p.debut_year,
            p.retire_year,
            p.notes
        FROM players p
        LEFT JOIN player_identities pi
          ON pi.player_id = p.id
         AND pi.is_primary = TRUE
        WHERE p.player_basic_id IS NULL
        ORDER BY p.id
    """
    path = output_dir / f"players_without_player_basic_mirror_{stamp}.csv"
    count = _write_csv(
        path,
        _rows(conn, sql),
        [
            "id",
            "kbo_person_id",
            "primary_name_kor",
            "primary_name_eng",
            "status",
            "birth_date",
            "debut_year",
            "retire_year",
            "notes",
        ],
    )
    return str(path), count


def _resolve_db_url(value: str | None) -> str:
    if value and value.startswith("env:"):
        env_name = value.split(":", 1)[1]
        resolved = os.getenv(env_name)
        if not resolved:
            raise RuntimeError(f"{env_name} is not set")
        return resolved
    return value or os.getenv("DATABASE_URL") or DATABASE_URL


def export_integrity_worklists(*, db_url: str, output_dir: Path, deletion_anomaly_only: bool = False) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    engine = create_engine(db_url)
    files: dict[str, dict[str, Any]] = {}
    try:
        with engine.connect() as conn:
            deletion_anomaly_exporters = (
                ("player_movements_unresolved_players", _export_unresolved_player_movements),
                ("players_without_player_basic_mirror", _export_players_without_mirror),
            )
            standard_exporters = (
                ()
                if deletion_anomaly_only
                else (
                    ("null_player_ids", _export_null_player_ids),
                    ("impossible_batting", _export_impossible_batting),
                    ("impossible_pitching", _export_impossible_pitching),
                )
            )
            for key, exporter in (*standard_exporters, *deletion_anomaly_exporters):
                path, count = exporter(conn, output_dir, stamp)
                files[key] = {"path": path, "rows": count}

            if not deletion_anomaly_only:
                for table_name, group_columns in (
                    ("game_batting_stats", "game_id, player_id"),
                    ("game_pitching_stats", "game_id, player_id"),
                    ("game_lineups", "game_id, player_id, team_code"),
                ):
                    path, count = _export_duplicate_groups(conn, output_dir, stamp, table_name, group_columns)
                    files[f"{table_name}_duplicates"] = {"path": path, "rows": count}

                for table_name in ("game_batting_stats", "game_pitching_stats", "game_lineups"):
                    path, count = _export_team_collisions(conn, output_dir, stamp, table_name)
                    files[f"{table_name}_team_collisions"] = {"path": path, "rows": count}
    finally:
        engine.dispose()

    return {"output_dir": str(output_dir), "files": files}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export read-only integrity cleanup worklists.")
    parser.add_argument("--db-url", default=None, help="Explicit SQLAlchemy DB URL. Defaults to DATABASE_URL/local DB.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for CSV worklists.")
    parser.add_argument(
        "--deletion-anomaly-only", action="store_true", help="Export only deletion-anomaly follow-up worklists."
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    db_url = _resolve_db_url(args.db_url)
    result = export_integrity_worklists(
        db_url=db_url,
        output_dir=Path(args.output_dir),
        deletion_anomaly_only=args.deletion_anomaly_only,
    )
    print(f"[REPORT] {result['output_dir']}")
    for key, info in result["files"].items():
        print(f"  {key}: rows={info['rows']} path={info['path']}")


if __name__ == "__main__":
    main()
