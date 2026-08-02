"""Read-only cross-database evidence report for selected player identities."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import bindparam, inspect, text

from src.constants import KST
from src.db.engine import create_engine_for_url

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PLAYER_IDS = (1352,)


def _tables(conn: Connection) -> set[str]:
    """Return lowercase table names."""
    return {str(name).lower() for name in inspect(conn).get_table_names()}


def _profiles(conn: Connection, player_ids: tuple[int, ...], names: tuple[str, ...]) -> list[dict[str, object]]:
    """Load profile rows matching selected IDs or names."""
    if "player_basic" not in _tables(conn):
        return []
    columns = {str(column["name"]).lower() for column in inspect(conn).get_columns("player_basic")}
    selected = [
        column for column in ("player_id", "name", "team", "position", "status", "debut_year") if column in columns
    ]
    if "player_id" not in selected or "name" not in selected:
        return []
    statement = (
        text(
            f"""
            SELECT {", ".join(selected)}
            FROM player_basic
            WHERE player_id IN :player_ids OR name IN :names
            ORDER BY player_id
            """,
        )
        .bindparams(bindparam("player_ids", expanding=True))
        .bindparams(bindparam("names", expanding=True))
    )
    return [
        dict(row) for row in conn.execute(statement, {"player_ids": list(player_ids), "names": list(names)}).mappings()
    ]


def _season_rows(conn: Connection, player_ids: tuple[int, ...], year: int) -> list[dict[str, object]]:
    """Load season pitching rows for selected IDs."""
    if "player_season_pitching" not in _tables(conn):
        return []
    statement = text(
        """
            SELECT player_id, season, league, team_code, innings_outs, innings_pitched, era, games
            FROM player_season_pitching
            WHERE season = :year AND player_id IN :player_ids
            ORDER BY player_id, team_code
            """,
    ).bindparams(bindparam("player_ids", expanding=True))
    return [dict(row) for row in conn.execute(statement, {"year": year, "player_ids": list(player_ids)}).mappings()]


def _game_rows(
    conn: Connection,
    player_ids: tuple[int, ...],
    names: tuple[str, ...],
    year: int,
) -> list[dict[str, object]]:
    """Aggregate local game pitching evidence by player/name/team."""
    if "game_pitching_stats" not in _tables(conn):
        return []
    statement = (
        text(
            """
            SELECT player_id, player_name, team_code,
                   COUNT(*) AS row_count,
                   COUNT(DISTINCT game_id) AS game_count,
                   COALESCE(SUM(innings_outs), 0) AS innings_outs,
                   COALESCE(SUM(innings_pitched), 0) AS innings_pitched
            FROM game_pitching_stats
            WHERE SUBSTR(game_id, 1, 4) = :year
              AND (player_id IN :player_ids OR player_name IN :names)
            GROUP BY player_id, player_name, team_code
            ORDER BY player_name, team_code, player_id
            """,
        )
        .bindparams(bindparam("player_ids", expanding=True))
        .bindparams(bindparam("names", expanding=True))
    )
    return [
        dict(row)
        for row in conn.execute(
            statement,
            {"year": str(year), "player_ids": list(player_ids), "names": list(names)},
        ).mappings()
    ]


def _movement_rows(conn: Connection, player_ids: tuple[int, ...], names: tuple[str, ...]) -> list[dict[str, object]]:
    """Load movement evidence when the table has compatible identity columns."""
    if "player_movements" not in _tables(conn):
        return []
    columns = {str(column["name"]).lower() for column in inspect(conn).get_columns("player_movements")}
    filters: list[str] = []
    params: dict[str, object] = {}
    bind_names: list[str] = []
    if "player_id" in columns:
        filters.append("player_id IN :player_ids")
        params["player_ids"] = list(player_ids)
        bind_names.append("player_ids")
    if "player_name" in columns:
        filters.append("player_name IN :names")
        params["names"] = list(names)
        bind_names.append("names")
    if not filters:
        return []
    selected = [
        column for column in ("movement_date", "section", "team_code", "player_id", "player_name") if column in columns
    ]
    order_by = " ORDER BY movement_date" if "movement_date" in columns else ""
    statement = text(f"SELECT {', '.join(selected)} FROM player_movements WHERE {' OR '.join(filters)}{order_by}")
    for bind_name in bind_names:
        statement = statement.bindparams(bindparam(bind_name, expanding=True))
    return [dict(row) for row in conn.execute(statement, params).mappings()]


def collect_identity_report(
    target_conn: Connection,
    local_conn: Connection,
    *,
    player_ids: tuple[int, ...],
    year: int,
) -> dict[str, object]:
    """Collect non-mutating target/local identity evidence."""
    target_profiles = _profiles(target_conn, player_ids, ())
    names = tuple(sorted({str(row["name"]) for row in target_profiles if row.get("name")}))
    local_profiles = _profiles(local_conn, player_ids, names)
    local_ids = tuple(sorted({int(row["player_id"]) for row in local_profiles}))
    all_local_ids = tuple(sorted(set(player_ids).union(local_ids)))
    target_tables = _tables(target_conn)
    local_tables = _tables(local_conn)
    return {
        "report_version": 1,
        "generated_at": datetime.now(KST).isoformat(),
        "read_only": True,
        "year": year,
        "player_ids": list(player_ids),
        "target_dialect": target_conn.dialect.name,
        "local_dialect": local_conn.dialect.name,
        "target_profiles": target_profiles,
        "local_profiles": local_profiles,
        "target_season_pitching": _season_rows(target_conn, player_ids, year),
        "local_season_pitching": _season_rows(local_conn, all_local_ids, year),
        "local_game_pitching": _game_rows(local_conn, all_local_ids, names, year),
        "target_movements": _movement_rows(target_conn, player_ids, names),
        "local_movements": _movement_rows(local_conn, all_local_ids, names),
        "target_roster_tables": sorted(table for table in target_tables if "roster" in table),
        "local_roster_tables": sorted(table for table in local_tables if "roster" in table),
    }


def _default_output(year: int) -> Path:
    """Return a timestamped evidence report path."""
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "data/audit" / f"oci_identity_investigation_{year}_{stamp}.json"


def main(argv: list[str] | None = None) -> int:
    """Run the identity evidence investigation CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2021)
    parser.add_argument("--player-id", action="append", type=int, dest="player_ids", default=[])
    parser.add_argument("--target-url", default=None)
    parser.add_argument("--local-url", default=None)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(argv)
    target_url = args.target_url or os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    local_url = args.local_url or os.getenv("DATABASE_URL")
    if not target_url:
        parser.error("target URL is required")
    if not local_url:
        parser.error("local URL is required")
    player_ids = tuple(sorted(set(args.player_ids or DEFAULT_PLAYER_IDS)))
    target_engine = create_engine_for_url(target_url)
    local_engine = create_engine_for_url(local_url)
    with target_engine.connect() as target_conn, local_engine.connect() as local_conn:
        report = collect_identity_report(target_conn, local_conn, player_ids=player_ids, year=args.year)
    output = args.output or _default_output(args.year)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"identity_investigation_report={output}")
    print(
        json.dumps(
            {"player_ids": list(player_ids), "target_roster_tables": report["target_roster_tables"]}, ensure_ascii=False
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
