from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


SNAPSHOT_QUERY = text(
    """
    WITH completed_games AS (
        SELECT CAST(SUBSTRING(game_id, 1, 4) AS INTEGER) AS season_year, game_id
        FROM game
        WHERE game_status = 'COMPLETED'
    ),
    event_games AS (
        SELECT CAST(SUBSTRING(game_id, 1, 4) AS INTEGER) AS season_year, game_id
        FROM game_events
        GROUP BY CAST(SUBSTRING(game_id, 1, 4) AS INTEGER), game_id
    ),
    pbp_games AS (
        SELECT CAST(SUBSTRING(game_id, 1, 4) AS INTEGER) AS season_year, game_id
        FROM game_play_by_play
        GROUP BY CAST(SUBSTRING(game_id, 1, 4) AS INTEGER), game_id
    )
    SELECT
        cg.season_year,
        COUNT(*) AS completed_games,
        COUNT(eg.game_id) AS event_games,
        COUNT(pg.game_id) AS play_by_play_games,
        COUNT(*) - COUNT(eg.game_id) AS completed_missing_events,
        COUNT(*) - COUNT(pg.game_id) AS completed_missing_play_by_play
    FROM completed_games cg
    LEFT JOIN event_games eg ON eg.season_year = cg.season_year AND eg.game_id = cg.game_id
    LEFT JOIN pbp_games pg ON pg.season_year = cg.season_year AND pg.game_id = cg.game_id
    WHERE cg.season_year BETWEEN :min_year AND :max_year
    GROUP BY cg.season_year
    ORDER BY cg.season_year DESC
    """
)


UNRESOLVED_QUERY = text(
    """
    WITH completed_games AS (
        SELECT
            CAST(SUBSTRING(g.game_id, 1, 4) AS INTEGER) AS season_year,
            g.game_date,
            g.game_id,
            g.home_team,
            g.away_team,
            g.season_id,
            ks.league_type_code,
            ks.league_type_name
        FROM game g
        LEFT JOIN kbo_seasons ks ON ks.season_id = g.season_id
        WHERE g.game_status = 'COMPLETED'
          AND CAST(SUBSTRING(g.game_id, 1, 4) AS INTEGER) BETWEEN :min_year AND :max_year
    ),
    event_games AS (
        SELECT DISTINCT game_id FROM game_events
    ),
    pbp_games AS (
        SELECT DISTINCT game_id FROM game_play_by_play
    )
    SELECT
        cg.season_year,
        cg.game_date,
        cg.game_id,
        cg.league_type_code,
        cg.league_type_name,
        cg.away_team,
        cg.home_team,
        CASE WHEN eg.game_id IS NULL THEN 1 ELSE 0 END AS missing_events,
        CASE WHEN pg.game_id IS NULL THEN 1 ELSE 0 END AS missing_play_by_play
    FROM completed_games cg
    LEFT JOIN event_games eg ON eg.game_id = cg.game_id
    LEFT JOIN pbp_games pg ON pg.game_id = cg.game_id
    WHERE eg.game_id IS NULL OR pg.game_id IS NULL
    ORDER BY cg.season_year DESC, cg.game_date DESC, cg.game_id DESC
    """
)


def _rows_to_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export relay backlog snapshot and unresolved game list")
    parser.add_argument("--db-env", default="OCI_DB_URL", help="Environment variable containing SQLAlchemy DB URL")
    parser.add_argument("--min-year", type=int, default=2001, help="Minimum season year")
    parser.add_argument("--max-year", type=int, default=2030, help="Maximum season year")
    parser.add_argument(
        "--snapshot-out",
        default="data/recovery/full_history_relay_snapshot_oci_20260415.csv",
        help="Output CSV path for year summary snapshot",
    )
    parser.add_argument(
        "--unresolved-out",
        default="data/recovery/full_history_unresolved_completed_games_oci_20260415.csv",
        help="Output CSV path for unresolved completed games",
    )
    args = parser.parse_args()

    load_dotenv(".env")
    db_url = os.getenv(args.db_env)
    if not db_url:
        raise SystemExit(f"{args.db_env} is not set")

    engine = create_engine(db_url)
    with engine.connect() as conn:
        snapshot_rows = [dict(row._mapping) for row in conn.execute(SNAPSHOT_QUERY, vars(args))]
        unresolved_rows = [dict(row._mapping) for row in conn.execute(UNRESOLVED_QUERY, vars(args))]

    _rows_to_csv(
        Path(args.snapshot_out),
        snapshot_rows,
        [
            "season_year",
            "completed_games",
            "event_games",
            "play_by_play_games",
            "completed_missing_events",
            "completed_missing_play_by_play",
        ],
    )
    _rows_to_csv(
        Path(args.unresolved_out),
        unresolved_rows,
        [
            "season_year",
            "game_date",
            "game_id",
            "league_type_code",
            "league_type_name",
            "away_team",
            "home_team",
            "missing_events",
            "missing_play_by_play",
        ],
    )

    print(f"snapshot={args.snapshot_out} rows={len(snapshot_rows)}")
    for row in snapshot_rows:
        print(
            "|".join(
                str(row[key])
                for key in (
                    "season_year",
                    "completed_games",
                    "event_games",
                    "play_by_play_games",
                    "completed_missing_events",
                    "completed_missing_play_by_play",
                )
            )
        )
    print(f"unresolved={args.unresolved_out} rows={len(unresolved_rows)}")


if __name__ == "__main__":
    main()
