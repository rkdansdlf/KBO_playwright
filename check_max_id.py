from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
with engine.connect() as conn:
    max_id = conn.execute(text('SELECT MAX(id) FROM game_play_by_play')).scalar()
    # Check if sequence exists and get its value
    try:
        curr_val = conn.execute(text("SELECT last_value FROM game_play_by_play_id_seq")).scalar()
    except Exception as e:
        curr_val = str(e)
    print(f'MAX ID: {max_id}, Sequence Current: {curr_val}')
