"""Quick script to check Futures league data in database."""

import sqlite3
from pathlib import Path

db_path = Path("data/kbo_dev.db")

if not db_path.exists():
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Count Futures batting records
cursor.execute("SELECT COUNT(*) FROM player_season_batting WHERE league='FUTURES'")
batting_count = cursor.fetchone()[0]
print(f"Futures Batting Records: {batting_count}")

# Count Futures pitching records
cursor.execute("SELECT COUNT(*) FROM player_season_pitching WHERE league='FUTURES'")
pitching_count = cursor.fetchone()[0]
print(f"Futures Pitching Records: {pitching_count}")

# Sample batting data
print("\nSample Futures Batting Data:")
cursor.execute("""
    SELECT pb.player_id, pb.name, psb.season, psb.league, psb.games, psb.avg
    FROM player_basic pb
    JOIN player_season_batting psb ON pb.player_id = psb.player_id
    WHERE psb.league='FUTURES'
    LIMIT 10
""")
for row in cursor.fetchall():
    print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]} | G:{row[4]} | AVG:{row[5]}")

# Sample pitching data
print("\nSample Futures Pitching Data:")
cursor.execute("""
    SELECT pb.player_id, pb.name, psp.season, psp.league, psp.games, psp.era
    FROM player_basic pb
    JOIN player_season_pitching psp ON pb.player_id = psp.player_id
    WHERE psp.league='FUTURES'
    LIMIT 10
""")
for row in cursor.fetchall():
    print(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]} | G:{row[4]} | ERA:{row[5]}")

conn.close()
