"""Read-only reconciliation audit for season-level pitching totals."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text

from src.db.engine import SessionLocal

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

PITCHING_FIELDS = (
    "innings_outs",
    "hits_allowed",
    "runs_allowed",
    "earned_runs",
    "home_runs_allowed",
    "walks_allowed",
    "strikeouts",
)

SOURCE_QUERIES = {
    "player_game": """
        SELECT
            pg.team_code AS team_code,
            COUNT(*) AS row_count,
            COUNT(DISTINCT pg.game_id) AS game_count,
            SUM(COALESCE(pg.innings_outs, 0)) AS innings_outs,
            SUM(COALESCE(pg.hits_allowed, 0)) AS hits_allowed,
            SUM(COALESCE(pg.runs_allowed, 0)) AS runs_allowed,
            SUM(COALESCE(pg.earned_runs, 0)) AS earned_runs,
            SUM(COALESCE(pg.home_runs_allowed, 0)) AS home_runs_allowed,
            SUM(COALESCE(pg.walks_allowed, 0)) AS walks_allowed,
            SUM(COALESCE(pg.strikeouts, 0)) AS strikeouts,
            SUM(CASE WHEN pg.team_code IS NULL OR TRIM(pg.team_code) = '' THEN 1 ELSE 0 END) AS null_team_rows
        FROM player_game_pitching pg
        JOIN game g ON g.game_id = pg.game_id
        JOIN kbo_seasons s ON s.season_id = g.season_id
        WHERE s.season_year = :year
        GROUP BY pg.team_code
    """,
    "game_pitching": """
        SELECT
            gps.team_code AS team_code,
            COUNT(*) AS row_count,
            COUNT(DISTINCT gps.game_id) AS game_count,
            SUM(COALESCE(gps.innings_outs, 0)) AS innings_outs,
            SUM(COALESCE(gps.hits_allowed, 0)) AS hits_allowed,
            SUM(COALESCE(gps.runs_allowed, 0)) AS runs_allowed,
            SUM(COALESCE(gps.earned_runs, 0)) AS earned_runs,
            SUM(COALESCE(gps.home_runs_allowed, 0)) AS home_runs_allowed,
            SUM(COALESCE(gps.walks_allowed, 0)) AS walks_allowed,
            SUM(COALESCE(gps.strikeouts, 0)) AS strikeouts,
            SUM(CASE WHEN gps.team_code IS NULL OR TRIM(gps.team_code) = '' THEN 1 ELSE 0 END) AS null_team_rows
        FROM game_pitching_stats gps
        JOIN game g ON g.game_id = gps.game_id
        JOIN kbo_seasons s ON s.season_id = g.season_id
        WHERE s.season_year = :year
        GROUP BY gps.team_code
    """,
    "team_season": """
        SELECT
            tsp.team_id AS team_code,
            COUNT(*) AS row_count,
            0 AS game_count,
            SUM(COALESCE(tsp.innings_outs, 0)) AS innings_outs,
            SUM(COALESCE(tsp.hits_allowed, 0)) AS hits_allowed,
            SUM(COALESCE(tsp.runs_allowed, 0)) AS runs_allowed,
            SUM(COALESCE(tsp.earned_runs, 0)) AS earned_runs,
            SUM(COALESCE(tsp.home_runs_allowed, 0)) AS home_runs_allowed,
            SUM(COALESCE(tsp.walks_allowed, 0)) AS walks_allowed,
            SUM(COALESCE(tsp.strikeouts, 0)) AS strikeouts,
            0 AS null_team_rows
        FROM team_season_pitching tsp
        WHERE tsp.season = :year AND tsp.league = 'REGULAR'
        GROUP BY tsp.team_id
    """,
}

DUPLICATE_QUERIES = {
    "player_game_same_game_player": """
        SELECT COUNT(*)
        FROM (
            SELECT pg.game_id, pg.player_id
            FROM player_game_pitching pg
            JOIN game g ON g.game_id = pg.game_id
            JOIN kbo_seasons s ON s.season_id = g.season_id
            WHERE s.season_year = :year
            GROUP BY pg.game_id, pg.player_id
            HAVING COUNT(*) > 1
        ) duplicates
    """,
    "game_pitching_same_game_player_appearance": """
        SELECT COUNT(*)
        FROM (
            SELECT gps.game_id, gps.player_id, gps.appearance_seq
            FROM game_pitching_stats gps
            JOIN game g ON g.game_id = gps.game_id
            JOIN kbo_seasons s ON s.season_id = g.season_id
            WHERE s.season_year = :year
            GROUP BY gps.game_id, gps.player_id, gps.appearance_seq
            HAVING COUNT(*) > 1
        ) duplicates
    """,
}


def _empty_totals() -> dict[str, int]:
    return dict.fromkeys(PITCHING_FIELDS, 0)


def _as_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _totals(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    totals = _empty_totals()
    for row in rows:
        for field in PITCHING_FIELDS:
            totals[field] += _as_int(row.get(field))
    return totals


def _diff(left: dict[str, int], right: dict[str, int]) -> dict[str, int]:
    return {field: left[field] - right[field] for field in PITCHING_FIELDS if left[field] != right[field]}


def _source_report(conn: Connection, source: str, year: int) -> dict[str, Any]:
    table = {
        "player_game": "player_game_pitching",
        "game_pitching": "game_pitching_stats",
        "team_season": "team_season_pitching",
    }[source]
    if not inspect(conn).has_table(table):
        return {"available": False, "table": table}

    rows = [dict(row) for row in conn.execute(text(SOURCE_QUERIES[source]), {"year": year}).mappings()]
    by_team = {
        str(row["team_code"] or ""): {
            "rows": _as_int(row["row_count"]),
            "games": _as_int(row["game_count"]),
            "totals": {field: _as_int(row[field]) for field in PITCHING_FIELDS},
            "null_team_rows": _as_int(row["null_team_rows"]),
        }
        for row in rows
    }
    return {
        "available": True,
        "table": table,
        "rows": sum(item["rows"] for item in by_team.values()),
        "games": sum(item["games"] for item in by_team.values()),
        "null_team_rows": sum(item["null_team_rows"] for item in by_team.values()),
        "totals": _totals(row["totals"] for row in by_team.values()),
        "by_team": by_team,
    }


def _duplicate_report(conn: Connection, year: int) -> dict[str, int]:
    result: dict[str, int] = {}
    for name, query in DUPLICATE_QUERIES.items():
        table = "player_game_pitching" if name.startswith("player_game") else "game_pitching_stats"
        if not inspect(conn).has_table(table):
            result[name] = -1
            continue
        result[name] = _as_int(conn.execute(text(query), {"year": year}).scalar_one())
    return result


def audit_pitching_global(conn: Connection, year: int) -> dict[str, Any]:
    """Compare game-level and season-level pitching totals without writes."""
    sources = {name: _source_report(conn, name, year) for name in SOURCE_QUERIES}
    comparisons: dict[str, Any] = {}
    player_game = sources["player_game"]
    for other_name in ("game_pitching", "team_season"):
        other = sources[other_name]
        key = f"player_game_vs_{other_name}"
        if not player_game["available"] or not other["available"]:
            comparisons[key] = {"available": False}
            continue
        differences = _diff(player_game["totals"], other["totals"])
        comparisons[key] = {
            "available": True,
            "diff": differences,
            "ok": not differences,
            "game_coverage": {
                "player_game": player_game["games"],
                other_name: other["games"],
                "complete": player_game["games"] >= other["games"] if other["games"] else True,
            },
        }

    game_comparison = comparisons["player_game_vs_game_pitching"]
    team_comparison = comparisons["player_game_vs_team_season"]
    if game_comparison.get("available") and not game_comparison["game_coverage"]["complete"]:
        classification = "player_game_source_incomplete"
    elif game_comparison.get("available") and not game_comparison["ok"]:
        classification = "game_level_aggregation_mismatch"
    elif team_comparison.get("available") and not team_comparison["ok"]:
        classification = "team_season_source_mismatch"
    elif game_comparison.get("available") or team_comparison.get("available"):
        classification = "reconciled"
    else:
        classification = "insufficient_sources"

    return {
        "year": year,
        "read_only": True,
        "sources": sources,
        "duplicates": _duplicate_report(conn, year),
        "comparisons": comparisons,
        "classification": classification,
    }


def main(argv: list[str] | None = None) -> int:
    """Run the read-only pitching reconciliation audit."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2021)
    parser.add_argument("--output")
    args = parser.parse_args(argv)

    with SessionLocal() as session:
        report = audit_pitching_global(session.connection(), args.year)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output:
        from pathlib import Path

        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        sys.stdout.write(f"Pitching audit report: {output}\n")
    else:
        sys.stdout.write(rendered)
    return 0 if report["classification"] == "reconciled" else 1


if __name__ == "__main__":
    raise SystemExit(main())
