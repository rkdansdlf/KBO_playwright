import sqlite3
from pathlib import Path


def check_anomalies(db_path):
    conn = sqlite3.connect(db_path)
    print(f"Checking anomalies for {db_path}...")

    # 1. Check for NULLs in mandatory fields (that might not have constraints)
    tables_to_check = {
        "player_basic": ["player_id", "name"],
        "player_season_batting": ["player_id", "season", "league", "team_code"],
        "player_season_pitching": ["player_id", "season", "league", "team_code"],
        "game": ["game_id", "game_date", "home_team", "away_team"],
        "game_batting_stats": ["game_id", "player_id", "team_code"],
        "game_pitching_stats": ["game_id", "player_id", "team_code"],
    }

    print("\n--- NULL/Empty Check ---")
    for table, cols in tables_to_check.items():
        for col in cols:
            query = f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR {col} = ''"
            count = conn.execute(query).fetchone()[0]
            if count > 0:
                print(f"[FAIL] {table}.{col} has {count} NULL or empty values.")
            else:
                print(f"[PASS] {table}.{col} looks good.")

    # 2. Check for logical duplicates (even if they don't violate PK/Unique)
    # E.g. Same player, same year, same team, same league in batting stats
    print("\n--- Logical Duplicate Check ---")
    dup_checks = {
        "player_season_batting": ["player_id", "season", "league", "level"],
        "player_season_pitching": ["player_id", "season", "league", "level"],
        "game": ["game_id"],
    }
    for table, cols in dup_checks.items():
        cols_str = ", ".join(cols)
        query = f"SELECT {cols_str}, COUNT(*) FROM {table} GROUP BY {cols_str} HAVING COUNT(*) > 1"
        dups = conn.execute(query).fetchall()
        if dups:
            print(f"[FAIL] {table} has {len(dups)} groups of duplicates based on {cols}.")
            for d in dups[:3]:
                print(f"  Example: {d}")
        else:
            print(f"[PASS] {table} has no logical duplicates on {cols}.")

    # 3. Check for statistical anomalies
    print("\n--- Statistical Sanity Check ---")
    # AVG > 1.0 (possible if plate appearances are low and logic is wrong, but usually 0-1)
    query = "SELECT player_id, season, avg FROM player_season_batting WHERE avg > 1.0"
    res = conn.execute(query).fetchall()
    if res:
        print(f"[WARN] player_season_batting has {len(res)} records with AVG > 1.0.")
        for r in res[:3]:
            print(f"  {r}")

    # Negative stats
    stat_cols = ["games", "plate_appearances", "at_bats", "hits", "home_runs", "runs"]
    for col in stat_cols:
        query = f"SELECT COUNT(*) FROM player_season_batting WHERE {col} < 0"
        count = conn.execute(query).fetchone()[0]
        if count > 0:
            print(f"[FAIL] player_season_batting has {count} records with negative {col}.")

    # 4. Team Code Consistency
    print("\n--- Team Code Consistency Check ---")
    # Check if team codes in stats match those in game table
    query = """
    SELECT DISTINCT team_code FROM game_batting_stats
    WHERE team_code NOT IN (SELECT team_id FROM teams)
    """
    res = conn.execute(query).fetchall()
    if res:
        print(f"[FAIL] game_batting_stats has unknown team codes: {res}")
    else:
        print("[PASS] game_batting_stats team codes are valid.")

    # 5. Player ID consistency between player_basic and stats
    print("\n--- Player ID Consistency Check ---")
    query = """
    SELECT COUNT(DISTINCT t.player_id)
    FROM player_season_batting t
    LEFT JOIN player_basic p ON t.player_id = p.player_id
    WHERE p.player_id IS NULL
    """
    count = conn.execute(query).fetchone()[0]
    if count > 0:
        print(f"[FAIL] player_season_batting has {count} player_ids missing in player_basic.")
    else:
        print("[PASS] player_season_batting player_ids all exist in player_basic.")

    conn.close()


if __name__ == "__main__":
    db_path = Path("data/kbo_dev.db")
    if db_path.exists():
        check_anomalies(db_path)
    else:
        print("DB not found.")
