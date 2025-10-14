"""Check database schema and tables."""
import sqlite3
from pathlib import Path

db_path = Path("data/kbo_dev.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# List all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print("Tables:")
for table in tables:
    print(f"  - {table[0]}")

# Check players table schema
print("\nPlayers table schema:")
cursor.execute("PRAGMA table_info(players)")
for col in cursor.fetchall():
    print(f"  {col[1]} ({col[2]})")

# Check player_season_batting schema
print("\nPlayer_season_batting table schema:")
cursor.execute("PRAGMA table_info(player_season_batting)")
for col in cursor.fetchall():
    print(f"  {col[1]} ({col[2]})")

# Count total records
cursor.execute("SELECT COUNT(*) FROM player_season_batting")
total_batting = cursor.fetchone()[0]
print(f"\nTotal batting records: {total_batting}")

cursor.execute("SELECT COUNT(*) FROM player_season_pitching")
total_pitching = cursor.fetchone()[0]
print(f"Total pitching records: {total_pitching}")

# Check if there are any records with source='PROFILE'
cursor.execute("SELECT COUNT(*) FROM player_season_batting WHERE source='PROFILE'")
profile_batting = cursor.fetchone()[0]
print(f"Batting records with source='PROFILE': {profile_batting}")

cursor.execute("SELECT COUNT(*) FROM player_season_pitching WHERE source='PROFILE'")
profile_pitching = cursor.fetchone()[0]
print(f"Pitching records with source='PROFILE': {profile_pitching}")

conn.close()
