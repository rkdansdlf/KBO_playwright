import os
import sqlite3
from sqlalchemy import create_engine, text

local_db_path = './data/kbo_dev.db'
if not os.path.exists(local_db_path):
    print(f'Local DB not found at {local_db_path}')
    exit(1)

conn = sqlite3.connect(local_db_path)
cur = conn.cursor()

def get_count(table_name):
    try:
        cur.execute(f'SELECT COUNT(*) FROM {table_name}')
        return cur.fetchone()[0]
    except Exception:
        return 'MISSING'

print('📊 Local DB Summary:')
print('\n=== Player Data ===')
print(f"  player_basic: {get_count('player_basic')}")
print(f"  player_season_batting: {get_count('player_season_batting')}")
print(f"  player_season_pitching: {get_count('player_season_pitching')}")

print('\n=== Game Data ===')
print(f"  games: {get_count('game')}")
print(f"  game_metadata: {get_count('game_metadata')}")
print(f"  game_inning_scores: {get_count('game_inning_scores')}")
print(f"  game_lineups: {get_count('game_lineups')}")
print(f"  game_batting_stats: {get_count('game_batting_stats')}")
print(f"  game_pitching_stats: {get_count('game_pitching_stats')}")
print(f"  game_summary: {get_count('game_summary')}")

print('\n=== Other Data ===')
print(f"  teams: {get_count('teams')}")
print(f"  kbo_seasons: {get_count('kbo_seasons')}")
print(f"  awards: {get_count('awards')}")

conn.close()
print('\n✅ Summary complete')
