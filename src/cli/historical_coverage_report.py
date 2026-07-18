"""Report historical game, detail, player-game, and PBP coverage."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text

from src.db.engine import get_oci_url

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.engine import Connection


COVERAGE_TABLES: tuple[str, ...] = (
    "game_lineups",
    "game_batting_stats",
    "game_pitching_stats",
    "player_game_batting",
    "player_game_pitching",
    "game_events",
    "game_play_by_play",
)
TERMINAL_GAME_STATUSES = frozenset({"COMPLETED", "DRAW"})
YEAR_RANGE_ERROR = "start_year must not exceed end_year"
UNKNOWN_GAME_YEAR_ERROR = "Unable to resolve game year for game ID"
TABLE_GAME_ID_QUERIES = {
    "game_lineups": text(
        "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id FROM game_lineups WHERE CAST(game_id AS TEXT) LIKE :prefix",
    ),
    "game_batting_stats": text(
        "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id FROM game_batting_stats "
        "WHERE CAST(game_id AS TEXT) LIKE :prefix",
    ),
    "game_pitching_stats": text(
        "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id FROM game_pitching_stats "
        "WHERE CAST(game_id AS TEXT) LIKE :prefix",
    ),
    "player_game_batting": text(
        "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id FROM player_game_batting "
        "WHERE CAST(game_id AS TEXT) LIKE :prefix",
    ),
    "player_game_pitching": text(
        "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id FROM player_game_pitching "
        "WHERE CAST(game_id AS TEXT) LIKE :prefix",
    ),
    "game_events": text(
        "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id FROM game_events WHERE CAST(game_id AS TEXT) LIKE :prefix",
    ),
    "game_play_by_play": text(
        "SELECT DISTINCT CAST(game_id AS TEXT) AS game_id FROM game_play_by_play "
        "WHERE CAST(game_id AS TEXT) LIKE :prefix",
    ),
}


def build_historical_coverage_report(
    conn: Connection,
    *,
    start_year: int,
    end_year: int,
) -> dict[str, Any]:
    """Build a read-only coverage report for a range of seasons.

    Args:
        conn: Database connection.
        start_year: First season year, inclusive.
        end_year: Last season year, inclusive.

    Returns:
        Structured coverage report containing missing game IDs.

    """
    if start_year > end_year:
        raise ValueError(YEAR_RANGE_ERROR)

    inspector = inspect(conn)
    available_tables = set(inspector.get_table_names())
    games = _load_games(conn, start_year, end_year, has_season_table="kbo_seasons" in available_tables)
    games_by_year: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for game in games:
        games_by_year[game["year"]].append(game)

    year_reports = []
    for year in range(start_year, end_year + 1):
        year_games = games_by_year.get(year, [])
        table_ids = _load_table_ids(conn, year, available_tables)
        year_report = _coverage_summary(year_games, table_ids)
        year_report["year"] = year
        year_report["series"] = _series_reports(year_games, table_ids)
        year_reports.append(year_report)

    return {
        "start_year": start_year,
        "end_year": end_year,
        "available_tables": sorted(available_tables & set(COVERAGE_TABLES)),
        "years": year_reports,
    }


def render_historical_coverage_report(report: dict[str, Any]) -> str:
    """Render a compact human-readable historical coverage report."""
    lines = [f"Historical coverage report: {report['start_year']}-{report['end_year']}"]
    for year_report in report["years"]:
        coverage = year_report["coverage"]
        lines.append(
            f"{year_report['year']}: parent={year_report['parent_games']}, "
            f"terminal={year_report['terminal_games']}, "
            f"batting={_coverage_text(coverage['game_batting_stats'])}, "
            f"pitching={_coverage_text(coverage['game_pitching_stats'])}, "
            f"events={_coverage_text(coverage['game_events'])}"
        )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the historical coverage report CLI."""
    parser = argparse.ArgumentParser(description="Report historical game-detail coverage")
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--json", action="store_true", help="Print the structured JSON report")
    parser.add_argument("--output", type=Path, help="Write the JSON report to this file")
    args = parser.parse_args(argv)

    if args.start_year > args.end_year:
        parser.error("--start-year must not exceed --end-year")

    database_url = args.database_url or os.getenv("DATABASE_URL") or get_oci_url()
    if not database_url:
        parser.error("database URL is required via --database-url, DATABASE_URL, or OCI_DB_URL")

    from sqlalchemy import create_engine

    engine = create_engine(database_url)
    with engine.connect() as conn:
        report = build_historical_coverage_report(
            conn,
            start_year=args.start_year,
            end_year=args.end_year,
        )

    json_report = json.dumps(report, ensure_ascii=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json_report + "\n", encoding="utf-8")
    print(json_report if args.json else render_historical_coverage_report(report))  # noqa: T201
    return 0


def _load_games(
    conn: Connection,
    start_year: int,
    end_year: int,
    *,
    has_season_table: bool,
) -> list[dict[str, Any]]:
    """Load parent games and their season-series metadata."""
    end_date = date(end_year + 1, 1, 1)
    query = (
        text(
            """
            SELECT CAST(g.game_id AS TEXT) AS game_id, g.game_date, g.game_status,
                   g.season_id, s.league_type_code AS series_code,
                   s.league_type_name AS series_name
            FROM game g
            LEFT JOIN kbo_seasons s ON s.season_id = g.season_id
            WHERE g.game_date >= :start_date AND g.game_date < :end_date
            ORDER BY g.game_date, g.game_id
            """,
        )
        if has_season_table
        else text(
            """
            SELECT CAST(g.game_id AS TEXT) AS game_id, g.game_date, g.game_status,
                   g.season_id, NULL AS series_code, NULL AS series_name
            FROM game g
            WHERE g.game_date >= :start_date AND g.game_date < :end_date
            ORDER BY g.game_date, g.game_id
            """,
        )
    )
    rows = conn.execute(
        query,
        {"start_date": date(start_year, 1, 1), "end_date": end_date},
    ).mappings()
    return [
        {
            "game_id": str(row["game_id"]),
            "year": _row_year(row, start_year, end_year),
            "status": str(row["game_status"] or "UNKNOWN"),
            "season_id": row["season_id"],
            "series_code": row["series_code"],
            "series_name": row["series_name"],
        }
        for row in rows
    ]


def _row_year(row: Mapping[str, object], start_year: int, end_year: int) -> int:
    """Resolve a report year from season metadata, date, or game ID."""
    season_year = row.get("season_year") if hasattr(row, "get") else None
    if season_year is not None:
        return int(season_year)
    game_date = row.get("game_date")
    if game_date is not None:
        if isinstance(game_date, date):
            return game_date.year
        return int(str(game_date)[:4])
    game_id = str(row["game_id"])
    year = int(game_id[:4])
    if start_year <= year <= end_year:
        return year
    error_message = f"{UNKNOWN_GAME_YEAR_ERROR}: {game_id}"
    raise ValueError(error_message)


def _load_table_ids(
    conn: Connection,
    year: int,
    available_tables: set[str],
) -> dict[str, set[str]]:
    """Load distinct game IDs present in each coverage table for one year."""
    table_ids: dict[str, set[str]] = {}
    for table in COVERAGE_TABLES:
        if table not in available_tables:
            table_ids[table] = set()
            continue
        rows = conn.execute(TABLE_GAME_ID_QUERIES[table], {"prefix": f"{year}%"})
        table_ids[table] = {str(row[0]) for row in rows}
    return table_ids


def _coverage_summary(
    games: list[dict[str, Any]],
    table_ids: dict[str, set[str]],
) -> dict[str, Any]:
    """Summarize coverage for a group of parent games."""
    status_counts = Counter(game["status"] for game in games)
    terminal_ids = {game["game_id"] for game in games if game["status"] in TERMINAL_GAME_STATUSES}
    return {
        "parent_games": len(games),
        "terminal_games": len(terminal_ids),
        "status_counts": dict(sorted(status_counts.items())),
        "coverage": _coverage_payload(terminal_ids, table_ids),
        "missing_game_ids": {table: sorted(terminal_ids - ids) for table, ids in table_ids.items()},
    }


def _series_reports(
    games: list[dict[str, Any]],
    table_ids: dict[str, set[str]],
) -> list[dict[str, Any]]:
    """Build coverage groups by series and game status."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for game in games:
        series = str(game["series_name"] or f"season_id:{game['season_id'] or 'unknown'}")
        grouped[(series, game["status"])].append(game)
    reports = []
    for (series, status), group_games in sorted(grouped.items()):
        summary = _coverage_summary(group_games, table_ids)
        reports.append({"series": series, "status": status, **summary})
    return reports


def _coverage_payload(
    terminal_ids: set[str],
    table_ids: dict[str, set[str]],
) -> dict[str, dict[str, float | int]]:
    """Calculate covered counts and percentages for each child table."""
    total = len(terminal_ids)
    return {
        table: {
            "covered_games": len(terminal_ids & ids),
            "target_games": total,
            "coverage_pct": round(len(terminal_ids & ids) / total * 100, 1) if total else 0.0,
        }
        for table, ids in table_ids.items()
    }


def _coverage_text(payload: dict[str, float | int]) -> str:
    """Format one coverage value for the text report."""
    return f"{payload['covered_games']}/{payload['target_games']} ({payload['coverage_pct']:.1f}%)"


if __name__ == "__main__":
    raise SystemExit(main())
