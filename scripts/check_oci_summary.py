import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('OCI_DB_URL'))
conn = engine.connect()

print('📊 OCI Database Summary:')
print('\n=== Player Data ===')
print(f"  player_basic: {conn.execute(text('SELECT COUNT(*) FROM player_basic')).fetchone()[0]}")
print(f"  player_season_batting: {conn.execute(text('SELECT COUNT(*) FROM player_season_batting')).fetchone()[0]}")
print(f"  player_season_pitching: {conn.execute(text('SELECT COUNT(*) FROM player_season_pitching')).fetchone()[0]}")
print(f"  2001 Batting: {conn.execute(text('SELECT COUNT(*) FROM player_season_batting WHERE season=2001')).fetchone()[0]}")
print(f"  2001 Pitching: {conn.execute(text('SELECT COUNT(*) FROM player_season_pitching WHERE season=2001')).fetchone()[0]}")

print('\n=== Game Data ===')
print(f"  games: {conn.execute(text('SELECT COUNT(*) FROM game')).fetchone()[0]}")
print(f"  game_metadata: {conn.execute(text('SELECT COUNT(*) FROM game_metadata')).fetchone()[0]}")
print(f"  game_inning_scores: {conn.execute(text('SELECT COUNT(*) FROM game_inning_scores')).fetchone()[0]}")
print(f"  game_lineups: {conn.execute(text('SELECT COUNT(*) FROM game_lineups')).fetchone()[0]}")
print(f"  game_batting_stats: {conn.execute(text('SELECT COUNT(*) FROM game_batting_stats')).fetchone()[0]}")
print(f"  game_pitching_stats: {conn.execute(text('SELECT COUNT(*) FROM game_pitching_stats')).fetchone()[0]}")
print(f"  game_summary: {conn.execute(text('SELECT COUNT(*) FROM game_summary')).fetchone()[0]}")

print('\n=== Other Data ===')
print(f"  teams: {conn.execute(text('SELECT COUNT(*) FROM teams')).fetchone()[0]}")
print(f"  kbo_seasons: {conn.execute(text('SELECT COUNT(*) FROM kbo_seasons')).fetchone()[0]}")
print(f"  awards: {conn.execute(text('SELECT COUNT(*) FROM awards')).fetchone()[0]}")

print('\n=== Game Years Distribution ===')
years = conn.execute(text('SELECT substr(game_id, 1, 4) as year, COUNT(*) as cnt FROM game GROUP BY substr(game_id, 1, 4) ORDER BY year DESC LIMIT 10')).fetchall()
for year, cnt in years:
    print(f"  {year}: {cnt} games")

conn.close()
print('\n✅ Summary complete')
