import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('OCI_DB_URL'))
with engine.connect() as conn:
    print("--- game_batting_stats ---")
    result = conn.execute(text("SELECT team_code, player_name, player_id, appearance_seq FROM game_batting_stats WHERE game_id = '20260517WONC0' AND player_name = '양현종';"))
    for row in result:
        print(row)
    
    print("\n--- game_lineups ---")
    result = conn.execute(text("SELECT team_code, player_name, player_id, appearance_seq FROM game_lineups WHERE game_id = '20260517WONC0' AND player_name = '양현종';"))
    for row in result:
        print(row)
