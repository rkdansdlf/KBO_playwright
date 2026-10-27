"""Report the season x column missing-data matrix for the operator handover.

Why this report exists
-----------------------
The operator asked for a season x column missing matrix and the queries behind
it. The matrix is a *suspicion list*, not a verdict: a column is flagged when
its season total is implausibly small next to that season's hit total, which
is a heuristic and not a proof that the source withheld the column.

Two facts about this database make the naive matrix misleading, and both are
the reason this report computes rather than tabulates:

* 1982~2000 rows carry a score and no body, and a date-ordered crawl sweep
  rewrote ``game_status`` on them. Counting only ``COMPLETED``/``DRAW`` dates
  therefore reports 1~16 games for those seasons, which is why the ranking
  qualification floor (30 PA / 90 outs) applies to every one of them. The
  games-per-team figure used here is derived from the schedule itself
  (``game`` rows x 2 / distinct teams) so it does not depend on a status
  column that was rewritten.
* A season can be *half* covered without any column being empty. 2009 has
  248 of 532 games with a batting body and 2010 has 10 of 591, while 2026
  covers every game but records no doubles or triples at all. Row counts and
  column sums are different questions, so this report answers both.

The report is read-only. It writes nothing and it does not fill anything:
the repository agent may not populate baseball data by external crawling or
by estimation, so the only remedy is the operator-provided data procedure
(``import_manual_baseball_data.py``).

Usage
-----
    python3 -m src.cli.reports.missing_data_matrix --start-year 1982 --end-year 2026
    python3 -m src.cli.reports.missing_data_matrix --json --output matrix.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import inspect, text

from src.constants import KBO_COLLECTED_START_YEAR
from src.db.engine import create_engine_for_url as create_engine

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.engine import Connection


#: Batting columns this report sums per season.
BATTING_COLUMNS: tuple[str, ...] = (
    "runs",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "rbi",
    "walks",
    "intentional_walks",
    "hbp",
    "strikeouts",
    "stolen_bases",
    "caught_stealing",
    "sacrifice_hits",
    "sacrifice_flies",
    "gdp",
    "plate_appearances",
    "at_bats",
)

#: Column total divided by the season hit total, below which a column is
#: flagged. The threshold is a *ratio*, so it scales with the season instead
#: of demanding an absolute count that a short season could never reach.
#:
#: Rare-count columns (walks, strikeouts) legitimately sit lower than the
#: common ones (hits, runs), so they carry a looser bound. A column that is
#: recorded at all produces a ratio far above these bounds; a column that was
#: never recorded produces exactly zero.
SUSPICION_RATIO: dict[str, float | None] = {
    "runs": 0.005,
    "hits": None,
    "doubles": 0.005,
    "triples": 0.005,
    "home_runs": 0.005,
    "rbi": 0.005,
    "walks": 0.05,
    "intentional_walks": 0.005,
    "hbp": 0.005,
    "strikeouts": 0.05,
    "stolen_bases": 0.005,
    "caught_stealing": 0.005,
    "sacrifice_hits": 0.005,
    "sacrifice_flies": 0.005,
    "gdp": 0.005,
    "plate_appearances": 0.5,
    "at_bats": 0.5,
}

#: Seasons whose boxscore bodies were never published.
SOURCE_LIMITED_END = KBO_COLLECTED_START_YEAR - 1

#: Minimum hits in a season before a ratio is meaningful.
MIN_HITS_FOR_RATIO = 100


def _season_totals_query(table: str, season_expression: str, where: str | None = None) -> str:
    """Build the per-season column-sum query for one table.

    Args:
        table: Table to aggregate.
        season_expression: SQL expression yielding the season year.
        where: Optional extra filter.

    Returns:
        SQL text with one row per season.

    """
    columns = ", ".join(f"COALESCE(SUM({column}), 0) AS {column}" for column in BATTING_COLUMNS)
    filter_sql = f"WHERE {where}" if where else ""
    return f"""
        SELECT {season_expression} AS season,
               COUNT(*) AS rows,
               {columns}
        FROM {table}
        {filter_sql}
        GROUP BY season
        ORDER BY season
    """  # noqa: S608


#: Season year of a ``game`` row, written so the same SQL runs on PostgreSQL
#: and SQLite. ``SUBSTRING`` over the ISO date text is portable where
#: ``EXTRACT`` and ``::int`` are not.
_GAME_SEASON = "CAST(SUBSTRING(CAST(game_date AS TEXT), 1, 4) AS INTEGER)"

#: Season year of a ``game_id``-keyed row. The game ID starts with the year.
_GAME_ID_SEASON = "CAST(SUBSTRING(CAST(game_id AS TEXT), 1, 4) AS INTEGER)"


def _season_games_query() -> str:
    """Build the per-season schedule query.

    The season length is derived from the schedule rather than from
    ``game_status``, because the 1982~2000 statuses were rewritten by a crawl
    sweep and no longer say which games were played.

    Returns:
        SQL text with one row per season.

    """
    return f"""
        WITH per_season AS (
            SELECT {_GAME_SEASON} AS season,
                   COUNT(*) AS game_rows
            FROM game
            GROUP BY season
        ),
        per_season_teams AS (
            SELECT {_GAME_SEASON} AS season,
                   COUNT(DISTINCT team) AS teams
            FROM (
                SELECT game_date, home_team AS team FROM game
                UNION ALL
                SELECT game_date, away_team AS team FROM game
            ) AS participants
            GROUP BY season
        )
        SELECT g.season,
               g.game_rows,
               t.teams,
               CASE WHEN t.teams > 0 THEN g.game_rows * 2.0 / t.teams ELSE 0 END AS games_per_team
        FROM per_season g
        JOIN per_season_teams t ON t.season = g.season
        ORDER BY g.season
    """  # noqa: S608


def _classify_column(column: str, value: int, hits: int) -> str:
    """Classify one season-column total as OK, MISSING, or SUSPECT.

    Args:
        column: Column name.
        value: Season total for the column.
        hits: Season total for hits, the ratio denominator.

    Returns:
        One of ``OK``, ``MISSING`` or ``SUSPECT``.

    """
    ratio_bound = SUSPICION_RATIO.get(column)
    if ratio_bound is None:
        return "OK"
    if hits < MIN_HITS_FOR_RATIO:
        return "OK"
    if value == 0:
        return "MISSING"
    if value / hits < ratio_bound:
        return "SUSPECT"
    return "OK"


def build_missing_data_matrix(conn: Connection) -> dict[str, Any]:
    """Build the season x column missing matrix.

    Args:
        conn: Database connection.

    Returns:
        Structured matrix payload.

    """
    seasons: dict[int, dict[str, Any]] = {}
    available_tables = set(inspect(conn).get_table_names())

    for row in conn.execute(text(_season_games_query())).mappings():
        season = int(row["season"])
        seasons.setdefault(season, {})["schedule"] = {
            "game_rows": int(row["game_rows"]),
            "teams": int(row["teams"]),
            "games_per_team": round(float(row["games_per_team"]), 2),
        }

    for label, table, season_expression, where in (
        ("player_season_batting", "player_season_batting", "season", "league = 'REGULAR'"),
        ("game_batting_stats", "game_batting_stats", _GAME_ID_SEASON, None),
    ):
        if table not in available_tables:
            continue
        query = text(_season_totals_query(table, season_expression, where))
        for row in conn.execute(query).mappings():
            season = int(row["season"])
            seasons.setdefault(season, {})[label] = _season_entry(row, where)

    return {
        "suspicion_basis": (
            "column season total divided by the season hit total, below the per-column bound; "
            "a recorded column scores far above the bound and an unrecorded one scores exactly zero"
        ),
        "ratio_bounds": {key: value for key, value in SUSPICION_RATIO.items() if value is not None},
        "seasons": [{"season": season, **payload} for season, payload in sorted(seasons.items())],
    }


def _season_entry(row: Mapping[str, Any], where: str | None) -> dict[str, Any]:
    """Build one season entry with per-column totals and classifications.

    Args:
        row: Result row.
        where: Optional extra filter applied by the caller.

    Returns:
        Season entry payload.

    """
    hits = int(row["hits"] or 0)
    totals = {column: int(row[column] or 0) for column in BATTING_COLUMNS}
    flags = {column: _classify_column(column, totals[column], hits) for column in BATTING_COLUMNS}
    return {
        "rows": int(row["rows"] or 0),
        "where": where,
        "totals": totals,
        "flags": flags,
        "flagged": sorted(column for column, flag in flags.items() if flag != "OK"),
    }


def render_missing_data_matrix(payload: dict[str, Any]) -> str:
    """Render the matrix as Markdown.

    Args:
        payload: Matrix payload.

    Returns:
        Markdown text.

    """
    lines = [
        "# KBO 결측 범위 — 시즌 x 컬럼 매트릭스 (운영자 인계)",
        "",
        f"판정: {payload['suspicion_basis']}.",
        "운영자 제공 데이터(`import_manual_baseball_data.py`)로만 보정 가능.",
        "",
    ]
    for season_payload in payload["seasons"]:
        season = season_payload["season"]
        schedule = season_payload.get("schedule")
        batting = season_payload.get("player_season_batting")
        game_batting = season_payload.get("game_batting_stats")
        if schedule is None and batting is None:
            continue
        lines.append(f"## {season}")
        if schedule:
            lines.append(
                f"- 일정: game 행 {schedule['game_rows']}, 팀 {schedule['teams']}, "
                f"팀당 경기 {schedule['games_per_team']}",
            )
        for label, entry in (("player_season_batting", batting), ("game_batting_stats", game_batting)):
            if entry is None:
                continue
            totals = entry["totals"]
            flagged = entry["flagged"]
            lines.append(
                f"- {label}: {entry['rows']}행, H={totals['hits']}, "
                f"HR={totals['home_runs']}, 2B={totals['doubles']}, 3B={totals['triples']}, "
                f"SB={totals['stolen_bases']}, BB={totals['walks']}, SO={totals['strikeouts']}, "
                f"HBP={totals['hbp']}, R={totals['runs']}",
            )
            lines.append(f"  - 의심 컬럼: {', '.join(flagged) if flagged else '없음'}")
        lines.append("")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the missing-data matrix CLI.

    Args:
        argv: Command-line arguments.

    Returns:
        Process exit code.

    """
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Report the season x column missing-data matrix")
    parser.add_argument("--database-url", default=None, help="Target database URL (default: DATABASE_URL)")
    parser.add_argument("--json", action="store_true", help="Print the structured JSON matrix")
    parser.add_argument("--output", type=Path, help="Write the JSON matrix to this file")
    parser.add_argument("--markdown-output", type=Path, help="Write the rendered Markdown matrix to this file")
    args = parser.parse_args(argv)

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("database URL is required via --database-url or DATABASE_URL")

    engine = create_engine(database_url)
    with engine.connect() as conn:
        payload = build_missing_data_matrix(conn)

    json_payload = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json_payload + "\n", encoding="utf-8")
    markdown = render_missing_data_matrix(payload)
    if args.markdown_output:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(markdown + "\n", encoding="utf-8")
    print(json_payload if args.json else markdown)  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
