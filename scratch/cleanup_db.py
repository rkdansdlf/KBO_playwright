import sqlite3

db_path = 'data/kbo_dev.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

tables = [
    'game', 'game_metadata', 'game_inning_scores', 'game_lineups', 
    'game_batting_stats', 'game_pitching_stats', 'game_events', 
    'game_summary', 'game_play_by_play'
]

# 1. Identify IDs with Korean characters or malformed patterns
bad_ids = set()
for table in tables:
    try:
        # Check for Korean characters
        cursor.execute(f"SELECT DISTINCT game_id FROM {table} WHERE game_id GLOB '*[가-힣]*'")
        for row in cursor.fetchall():
            bad_ids.add(row[0])
        
        # Check for malformed numeric patterns like 20260414KT00NC0 (extra zeros)
        # Assuming canonical is 10 chars, check for ones with '00' in middle
        cursor.execute(f"SELECT DISTINCT game_id FROM {table} WHERE game_id LIKE '20260414%00%'")
        for row in cursor.fetchall():
            bad_ids.add(row[0])
    except Exception as e:
        print(f"Error checking {table}: {e}")

print(f"Found {len(bad_ids)} bad game_ids: {bad_ids}")

# 2. Delete these IDs from all tables
for game_id in bad_ids:
    print(f"Deleting {game_id}...")
    for table in tables:
        cursor.execute(f"DELETE FROM {table} WHERE game_id = ?", (game_id,))

# 3. Check for any other issues (e.g. season_id IS NULL for 2026)
# Actually, re-crawling will fix season_id mismatches, but let's check.
cursor.execute("SELECT game_id FROM game WHERE season_id IS NULL AND game_date >= '2026-01-01'")
null_season_ids = cursor.fetchall()
if null_season_ids:
    print(f"Found games with NULL season_id: {[r[0] for r in null_season_ids]}")

conn.commit()
conn.close()
print("Cleanup complete.")
