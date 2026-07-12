import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)


def _check_nulls(conn):
    tables_to_check = {
        "player_basic": ["player_id", "name"],
        "player_season_batting": ["player_id", "season", "league", "team_code"],
        "player_season_pitching": ["player_id", "season", "league", "team_code"],
        "game": ["game_id", "game_date", "home_team", "away_team"],
        "game_batting_stats": ["game_id", "player_id", "team_code"],
        "game_pitching_stats": ["game_id", "player_id", "team_code"],
    }
    logger.info("\n--- NULL/Empty Check ---")
    for table, cols in tables_to_check.items():
        for col in cols:
            count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR {col} = ''").fetchone()[0]
            logger.info(f"{'[FAIL]' if count > 0 else '[PASS]'} {table}.{col} has {count} NULL or empty values.")


def _check_duplicates(conn):
    logger.info("\n--- Logical Duplicate Check ---")
    for table, cols in {
        "player_season_batting": ["player_id", "season", "league", "level"],
        "player_season_pitching": ["player_id", "season", "league", "level"],
        "game": ["game_id"],
    }.items():
        cols_str = ", ".join(cols)
        dups = conn.execute(
            f"SELECT {cols_str}, COUNT(*) FROM {table} GROUP BY {cols_str} HAVING COUNT(*) > 1"
        ).fetchall()
        if dups:
            logger.info(f"[FAIL] {table} has {len(dups)} groups of duplicates based on {cols}.")
            for d in dups[:3]:
                logger.info(f"  Example: {d}")
        else:
            logger.info(f"[PASS] {table} has no logical duplicates on {cols}.")


def _check_stat_anomalies(conn):
    logger.info("\n--- Statistical Sanity Check ---")
    res = conn.execute("SELECT player_id, season, avg FROM player_season_batting WHERE avg > 1.0").fetchall()
    if res:
        logger.info(f"[WARN] player_season_batting has {len(res)} records with AVG > 1.0.")
        for r in res[:3]:
            logger.info(f"  {r}")
    for col in ["games", "plate_appearances", "at_bats", "hits", "home_runs", "runs"]:
        count = conn.execute(f"SELECT COUNT(*) FROM player_season_batting WHERE {col} < 0").fetchone()[0]
        if count > 0:
            logger.info(f"[FAIL] player_season_batting has {count} records with negative {col}.")


def check_anomalies(db_path):
    conn = sqlite3.connect(db_path)
    logger.info(f"Checking anomalies for {db_path}...")
    _check_nulls(conn)
    _check_duplicates(conn)
    _check_stat_anomalies(conn)

    # 4. Team Code Consistency
    logger.info("\n--- Team Code Consistency Check ---")
    # Check if team codes in stats match those in game table
    query = """
    SELECT DISTINCT team_code FROM game_batting_stats
    WHERE team_code NOT IN (SELECT team_id FROM teams)
    """
    res = conn.execute(query).fetchall()
    if res:
        logger.info(f"[FAIL] game_batting_stats has unknown team codes: {res}")
    else:
        logger.info("[PASS] game_batting_stats team codes are valid.")

    # 5. Player ID consistency between player_basic and stats
    logger.info("\n--- Player ID Consistency Check ---")
    query = """
    SELECT COUNT(DISTINCT t.player_id)
    FROM player_season_batting t
    LEFT JOIN player_basic p ON t.player_id = p.player_id
    WHERE p.player_id IS NULL
    """
    count = conn.execute(query).fetchone()[0]
    if count > 0:
        logger.info(f"[FAIL] player_season_batting has {count} player_ids missing in player_basic.")
    else:
        logger.info("[PASS] player_season_batting player_ids all exist in player_basic.")

    conn.close()


if __name__ == "__main__":
    db_path = Path("data/kbo_dev.db")
    if db_path.exists():
        check_anomalies(db_path)
    else:
        logger.info("DB not found.")
