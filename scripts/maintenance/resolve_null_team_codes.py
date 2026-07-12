#!/usr/bin/env python3
"""Conservatively resolve NULL team_code values in player_season_batting/pitching tables.

Heuristics used:
1. Game-level stats (batting/pitching) matching the same player and season.
2. Other season stats (fielding/baserunning) matching the same player and season.
3. Game lineups matching the same player and season.
4. Roster history: Check team_daily_roster and roster_transactions for that season.
5. Single-career team code: If the player has only ever played for one team across their entire career.
6. Adjacent seasons team code: If the player played for the same team in season - 1 and season + 1.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import DATABASE_URL, SessionLocal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NullRow:
    table_name: str
    player_id: int
    player_name: str
    season: int
    league: str
    level: str


def _sqlite_path_from_url(db_url: str) -> Path | None:
    if not db_url.startswith("sqlite:///"):
        return None
    return Path(db_url.removeprefix("sqlite:///"))


def _backup_sqlite_database(db_url: str, output_dir: Path) -> Path | None:
    db_path = _sqlite_path_from_url(db_url)
    if db_path is None or not db_path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = output_dir / f"{db_path.name}.backup_before_team_code_resolve_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_path, backup_path)
    return backup_path


def find_null_rows(session: Session, year: int | None = None) -> list[NullRow]:
    """Find all rows with NULL team_code in player_season_batting and pitching tables."""
    rows: list[NullRow] = []

    tables = ["player_season_batting", "player_season_pitching"]
    for table in tables:
        sql = f"""
            SELECT ps.player_id, pb.name, ps.season, ps.league, ps.level
            FROM {table} ps
            LEFT JOIN player_basic pb ON ps.player_id = pb.player_id
            WHERE ps.team_code IS NULL OR ps.team_code = ''
        """
        params: dict[str, Any] = {}
        if year is not None:
            sql += " AND ps.season = :year"
            params["year"] = year

        result = session.execute(text(sql), params).fetchall()
        for r in result:
            rows.append(
                NullRow(
                    table_name=table,
                    player_id=r[0],
                    player_name=r[1] or f"Player_{r[0]}",
                    season=r[2],
                    league=r[3],
                    level=r[4],
                ),
            )
    return rows


def resolve_from_game_stats(session: Session, player_id: int, season: int, is_pitcher: bool) -> str | None:
    """Heuristic 1: Get unique team_code from game stats in that season."""
    table = "game_pitching_stats" if is_pitcher else "game_batting_stats"
    sql = f"""
        SELECT DISTINCT gs.team_code
        FROM {table} gs
        JOIN game g ON gs.game_id = g.game_id
        JOIN kbo_seasons s ON g.season_id = s.season_id
        WHERE gs.player_id = :player_id
          AND s.season_year = :season
          AND gs.team_code IS NOT NULL
          AND gs.team_code != ''
    """
    results = session.execute(text(sql), {"player_id": player_id, "season": season}).fetchall()
    if len(results) == 1:
        return str(results[0][0])
    return None


def resolve_from_other_season_stats(session: Session, player_id: int, season: int) -> str | None:
    """Heuristic 2: Check fielding / baserunning stats for the same season."""
    sql_fielding = """
        SELECT DISTINCT team_id
        FROM player_season_fielding
        WHERE player_id = :player_id AND year = :season AND team_id IS NOT NULL AND team_id != ''
    """
    sql_baserun = """
        SELECT DISTINCT team_id
        FROM player_season_baserunning
        WHERE player_id = :player_id AND year = :season AND team_id IS NOT NULL AND team_id != ''
    """

    codes = set()
    for sql in (sql_fielding, sql_baserun):
        res = session.execute(text(sql), {"player_id": player_id, "season": season}).fetchall()
        for r in res:
            codes.add(str(r[0]))

    if len(codes) == 1:
        return list(codes)[0]
    return None


def resolve_from_lineups(session: Session, player_id: int, season: int) -> str | None:
    """Heuristic 3: Check game_lineups for that season."""
    sql = """
        SELECT DISTINCT gl.team_code
        FROM game_lineups gl
        JOIN game g ON gl.game_id = g.game_id
        JOIN kbo_seasons s ON g.season_id = s.season_id
        WHERE gl.player_id = :player_id
          AND s.season_year = :season
          AND gl.team_code IS NOT NULL
          AND gl.team_code != ''
    """
    res = session.execute(text(sql), {"player_id": player_id, "season": season}).fetchall()
    if len(res) == 1:
        return str(res[0][0])
    return None


def resolve_from_rosters(session: Session, player_id: int, season: int) -> str | None:
    """Heuristic 4: Check team_daily_roster and roster_transactions for that season."""
    sql_daily = """
        SELECT DISTINCT team_code
        FROM team_daily_roster
        WHERE player_id = :player_id
          AND roster_date >= :start_date
          AND roster_date <= :end_date
          AND team_code IS NOT NULL
          AND team_code != ''
    """
    sql_trans = """
        SELECT DISTINCT team_id
        FROM roster_transactions
        WHERE player_id = :player_id
          AND transaction_date >= :start_date
          AND transaction_date <= :end_date
          AND team_id IS NOT NULL
          AND team_id != ''
    """

    start_date = date(season, 1, 1)
    end_date = date(season, 12, 31)

    codes = set()
    for sql in (sql_daily, sql_trans):
        res = session.execute(
            text(sql),
            {"player_id": player_id, "start_date": start_date, "end_date": end_date},
        ).fetchall()
        for r in res:
            codes.add(str(r[0]))

    if len(codes) == 1:
        return list(codes)[0]
    return None


def resolve_from_single_career_team(session: Session, player_id: int) -> str | None:
    """Heuristic 5: If player has only ever played for one team in their whole career."""
    queries = [
        "SELECT DISTINCT team_id FROM player_season_fielding WHERE player_id = :player_id",
        "SELECT DISTINCT team_id FROM player_season_baserunning WHERE player_id = :player_id",
        "SELECT DISTINCT team_code FROM player_season_batting WHERE player_id = :player_id",
        "SELECT DISTINCT team_code FROM player_season_pitching WHERE player_id = :player_id",
        "SELECT DISTINCT team_code FROM game_batting_stats WHERE player_id = :player_id",
        "SELECT DISTINCT team_code FROM game_pitching_stats WHERE player_id = :player_id",
        "SELECT DISTINCT team_code FROM game_lineups WHERE player_id = :player_id",
        "SELECT DISTINCT team_code FROM team_daily_roster WHERE player_id = :player_id",
        "SELECT DISTINCT team_id FROM roster_transactions WHERE player_id = :player_id",
    ]

    codes = set()
    for sql in queries:
        res = session.execute(text(sql), {"player_id": player_id}).fetchall()
        for r in res:
            val = r[0]
            if val and val != "None" and val != "":
                codes.add(str(val))

    if len(codes) == 1:
        return list(codes)[0]
    return None


def resolve_from_adjacent_seasons(session: Session, player_id: int, season: int, table_name: str) -> str | None:
    """Heuristic 6: Match team code of preceding (N-1) and succeeding (N+1) seasons."""
    sql = f"""
        SELECT season, team_code
        FROM {table_name}
        WHERE player_id = :player_id
          AND season IN (:prev, :next)
          AND team_code IS NOT NULL
          AND team_code != ''
    """
    res = session.execute(text(sql), {"player_id": player_id, "prev": season - 1, "next": season + 1}).fetchall()
    matched = {str(r[1]) for r in res}
    if len(matched) == 1:
        seasons_present = {r[0] for r in res}
        if {season - 1, season + 1}.issubset(seasons_present):
            return list(matched)[0]
    return None


def resolve_team_codes(session: Session, year: int | None = None, apply: bool = False) -> dict[str, int]:
    """Audit and resolve NULL team codes using multiple heuristics."""
    null_rows = find_null_rows(session, year)
    logger.info("Found %s rows with NULL team_code.", len(null_rows))

    stats = {
        "total": len(null_rows),
        "resolved_game_stats": 0,
        "resolved_season_stats": 0,
        "resolved_lineups": 0,
        "resolved_rosters": 0,
        "resolved_single_career": 0,
        "resolved_adjacent": 0,
        "unresolved": 0,
        "updated": 0,
    }

    updates: list[tuple[NullRow, str, str]] = []

    for r in null_rows:
        is_pitcher = r.table_name == "player_season_pitching"

        # Heuristic 1: Game Stats
        team = resolve_from_game_stats(session, r.player_id, r.season, is_pitcher)
        if team:
            updates.append((r, team, "game_stats"))
            stats["resolved_game_stats"] += 1
            continue

        # Heuristic 2: Other Season Stats
        team = resolve_from_other_season_stats(session, r.player_id, r.season)
        if team:
            updates.append((r, team, "other_season_stats"))
            stats["resolved_season_stats"] += 1
            continue

        # Heuristic 3: Lineups
        team = resolve_from_lineups(session, r.player_id, r.season)
        if team:
            updates.append((r, team, "lineups"))
            stats["resolved_lineups"] += 1
            continue

        # Heuristic 4: Roster Tables
        team = resolve_from_rosters(session, r.player_id, r.season)
        if team:
            updates.append((r, team, "roster_tables"))
            stats["resolved_rosters"] += 1
            continue

        # Heuristic 5: Career unique team
        team = resolve_from_single_career_team(session, r.player_id)
        if team:
            updates.append((r, team, "single_career"))
            stats["resolved_single_career"] += 1
            continue

        # Heuristic 6: Adjacent seasons
        team = resolve_from_adjacent_seasons(session, r.player_id, r.season, r.table_name)
        if team:
            updates.append((r, team, "adjacent_seasons"))
            stats["resolved_adjacent"] += 1
            continue

        stats["unresolved"] += 1

    for row, team, method in updates:
        logger.info(
            "   [Resolve] Player %s (%s, ID: %s) in %s Season %s -> %s (via %s)",
            row.player_name,
            row.table_name.split("_")[-1],
            row.player_id,
            row.league,
            row.season,
            team,
            method,
        )

        if apply:
            sql = f"""
                UPDATE {row.table_name}
                SET team_code = :team, updated_at = :now
                WHERE player_id = :player_id
                  AND season = :season
                  AND league = :league
                  AND level = :level
            """
            session.execute(
                text(sql),
                {
                    "team": team,
                    "now": datetime.now(),
                    "player_id": row.player_id,
                    "season": row.season,
                    "league": row.league,
                    "level": row.level,
                },
            )
            stats["updated"] += 1

    return stats


def main() -> int:
    """Main CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Resolve NULL team_code values in player_season tables")
    parser.add_argument("--apply", action="store_true", help="Apply updates to database (commits transaction)")
    parser.add_argument("--year", type=int, help="Limit resolution to a specific year")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "data", help="Backup output directory")
    args = parser.parse_args()

    load_dotenv()

    apply = args.apply
    if apply:
        backup = _backup_sqlite_database(DATABASE_URL, args.output_dir)
        if backup:
            logger.info("💾 Database backup created: %s", backup)
    else:
        logger.info("🔍 Running in DRY-RUN mode. No database changes will be committed.")

    with SessionLocal() as session:
        try:
            stats = resolve_team_codes(session, year=args.year, apply=apply)
            if apply:
                session.commit()
                logger.info("🎉 Resolution committed successfully! Records updated: %s", stats["updated"])
            else:
                logger.info(
                    "🔍 Dry-run complete. Would update %s records.",
                    len(find_null_rows(session, args.year)) - stats["unresolved"],
                )

            logger.info("\n📊 Execution Summary:")
            for k, v in stats.items():
                logger.info("   %-25s: %s", k, v)
        except SQLAlchemyError:
            session.rollback()
            logger.exception("❌ Database operation failed")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
