from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
with engine.connect() as conn:
    max_id = conn.execute(text("SELECT MAX(id) FROM game_play_by_play")).scalar()
    curr_val = conn.execute(text("SELECT last_value FROM game_play_by_play_id_seq")).scalar()
    print(f"MAX ID: {max_id}, Sequence: {curr_val}")
    
    # Check if 32082477 exists
    exists = conn.execute(text("SELECT game_id FROM game_play_by_play WHERE id = 32082477")).fetchone()
    print(f"ID 32082477 exists for game: {exists}")
