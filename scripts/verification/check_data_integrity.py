
import sqlite3
from pathlib import Path

def check_integrity():
    db_path = Path("data/kbo_dev.db")
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=== Database Integrity Check ===")

    # 1. Check Foreign Key PRAGMA
    cursor.execute("PRAGMA foreign_keys")
    fk_status = cursor.fetchone()[0]
    print(f"Foreign Keys enabled (PRAGMA): {'ON' if fk_status else 'OFF'}")

    # 2. Check for orphaned records in GameBattingStat
    print("\nChecking for orphaned records...")
    cursor.execute("""
        SELECT COUNT(*) FROM game_batting_stats 
        WHERE game_id NOT IN (SELECT game_id FROM game)
    """)
    orphaned_batting_games = cursor.fetchone()[0]
    print(f"Orphaned game_batting_stats (invalid game_id): {orphaned_batting_games}")

    cursor.execute("""
        SELECT COUNT(*) FROM game_pitching_stats 
        WHERE game_id NOT IN (SELECT game_id FROM game)
    """)
    orphaned_pitching_games = cursor.fetchone()[0]
    print(f"Orphaned game_pitching_stats (invalid game_id): {orphaned_pitching_games}")

    # 3. Check for players in stats but missing in player_basic
    cursor.execute("""
        SELECT COUNT(DISTINCT player_id) FROM game_batting_stats 
        WHERE player_id IS NOT NULL AND player_id NOT IN (SELECT player_id FROM player_basic)
    """)
    missing_players_batting = cursor.fetchone()[0]
    print(f"Unique players in game_batting_stats missing from player_basic: {missing_players_batting}")

    cursor.execute("""
        SELECT COUNT(DISTINCT player_id) FROM game_pitching_stats 
        WHERE player_id IS NOT NULL AND player_id NOT IN (SELECT player_id FROM player_basic)
    """)
    missing_players_pitching = cursor.fetchone()[0]
    print(f"Unique players in game_pitching_stats missing from player_basic: {missing_players_pitching}")

    cursor.execute("""
        SELECT COUNT(DISTINCT player_id) FROM player_season_batting 
        WHERE player_id NOT IN (SELECT player_id FROM player_basic)
    """)
    missing_players_season_batting = cursor.fetchone()[0]
    print(f"Unique players in player_season_batting missing from player_basic: {missing_players_season_batting}")

    # 4. Check for score consistency
    print("\nChecking for score consistency...")
    cursor.execute("""
        SELECT g.game_id, g.home_score, SUM(i.runs) as calc_score
        FROM game g
        JOIN game_inning_scores i ON g.game_id = i.game_id
        WHERE i.team_side = 'home'
        GROUP BY g.game_id
        HAVING g.home_score != calc_score
    """)
    mismatched_home_scores = cursor.fetchall()
    print(f"Games with mismatched home score totals: {len(mismatched_home_scores)}")
    for row in mismatched_home_scores[:5]:
        print(f"  Game {row[0]}: DB score {row[1]}, Calculated {row[2]}")

    # 5. Check for null player_id in stats (Critical for analysis)
    cursor.execute("SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL")
    null_batting_player = cursor.fetchone()[0]
    print(f"\nNull player_id in game_batting_stats: {null_batting_player}")

    cursor.execute("SELECT COUNT(*) FROM game_pitching_stats WHERE player_id IS NULL")
    null_pitching_player = cursor.fetchone()[0]
    print(f"Null player_id in game_pitching_stats: {null_pitching_player}")

    # 6. Check for duplicate stats (same game, same player, same seq)
    # The UniqueConstraint should prevent this, but let's verify if any slipped through 
    # (e.g. if constraints weren't there initially)
    cursor.execute("""
        SELECT game_id, player_id, appearance_seq, COUNT(*) 
        FROM game_batting_stats 
        GROUP BY game_id, player_id, appearance_seq 
        HAVING COUNT(*) > 1
    """)
    duplicate_batting = cursor.fetchall()
    print(f"\nDuplicate game_batting_stats records: {len(duplicate_batting)}")

    conn.close()

if __name__ == "__main__":
    check_integrity()
