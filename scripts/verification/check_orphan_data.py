import sqlite3
from pathlib import Path

def check_orphans():
    db_path = Path("data/kbo_dev.db")
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=== Orphan Data Verification Report ===\n")

    # 1. PRAGMA foreign_key_check
    print("Checking Database-level Foreign Key Constraints (PRAGMA foreign_key_check)...")
    cursor.execute("PRAGMA foreign_key_check")
    violations = cursor.fetchall()
    if violations:
        print(f"Found {len(violations)} FK violations:")
        for v in violations:
            print(f"  - Table: {v[0]}, RowID: {v[1]}, Parent: {v[2]}, FKey Index: {v[3]}")
    else:
        print("  OK: No database-level FK violations found.")

    # 2. Targeted Orphan Checks
    checks = [
        # Game Details -> Game
        {
            "name": "Game Batting Stats -> Game",
            "query": "SELECT COUNT(*) FROM game_batting_stats AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL",
            "sample_query": "SELECT DISTINCT t.game_id FROM game_batting_stats AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL LIMIT 5"
        },
        {
            "name": "Game Pitching Stats -> Game",
            "query": "SELECT COUNT(*) FROM game_pitching_stats AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL",
            "sample_query": "SELECT DISTINCT t.game_id FROM game_pitching_stats AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL LIMIT 5"
        },
        {
            "name": "Game Metadata -> Game",
            "query": "SELECT COUNT(*) FROM game_metadata AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL",
            "sample_query": "SELECT DISTINCT t.game_id FROM game_metadata AS t LEFT JOIN game AS p ON t.game_id = p.game_id WHERE p.game_id IS NULL LIMIT 5"
        },
        # Player Stats -> Player Basic
        {
            "name": "Player Season Batting -> Player Basic",
            "query": "SELECT COUNT(*) FROM player_season_batting AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL",
            "sample_query": "SELECT DISTINCT t.player_id FROM player_season_batting AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL LIMIT 5"
        },
        {
            "name": "Player Season Pitching -> Player Basic",
            "query": "SELECT COUNT(*) FROM player_season_pitching AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL",
            "sample_query": "SELECT DISTINCT t.player_id FROM player_season_pitching AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL LIMIT 5"
        },
        # Game Detail Player ID -> Player Basic
        {
            "name": "Game Batting Player -> Player Basic",
            "query": "SELECT COUNT(*) FROM game_batting_stats AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL AND t.player_id IS NOT NULL",
            "sample_query": "SELECT DISTINCT t.player_id FROM game_batting_stats AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL AND t.player_id IS NOT NULL LIMIT 5"
        },
        {
            "name": "Game Pitching Player -> Player Basic",
            "query": "SELECT COUNT(*) FROM game_pitching_stats AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL AND t.player_id IS NOT NULL",
            "sample_query": "SELECT DISTINCT t.player_id FROM game_pitching_stats AS t LEFT JOIN player_basic AS p ON t.player_id = p.player_id WHERE p.player_id IS NULL AND t.player_id IS NOT NULL LIMIT 5"
        },
        # Records -> Teams
        {
            "name": "Game Home Team -> Teams",
            "query": "SELECT COUNT(*) FROM game AS t LEFT JOIN teams AS p ON t.home_team = p.team_id WHERE p.team_id IS NULL",
            "sample_query": "SELECT DISTINCT t.home_team FROM game AS t LEFT JOIN teams AS p ON t.home_team = p.team_id WHERE p.team_id IS NULL LIMIT 5"
        },
        {
            "name": "Game Away Team -> Teams",
            "query": "SELECT COUNT(*) FROM game AS t LEFT JOIN teams AS p ON t.away_team = p.team_id WHERE p.team_id IS NULL",
            "sample_query": "SELECT DISTINCT t.away_team FROM game AS t LEFT JOIN teams AS p ON t.away_team = p.team_id WHERE p.team_id IS NULL LIMIT 5"
        }
    ]

    print("\nExecuting Targeted Orphan Checks...")
    for check in checks:
        try:
            cursor.execute(check["query"])
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"  ❌ FAIL {check['name']}: {count} orphans found")
                cursor.execute(check["sample_query"])
                orphan_ids = [str(r[0]) for r in cursor.fetchall()]
                print(f"    Sample Orphan IDs: {', '.join(orphan_ids)}")
            else:
                print(f"  ✅ PASS {check['name']}: 0 orphans found")
        except sqlite3.OperationalError as e:
            print(f"  ⚠️  ERROR {check['name']}: {e}")

    conn.close()
    print("\nVerification complete.")

if __name__ == "__main__":
    check_orphans()
