from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
with engine.connect() as conn:
    row = conn.execute(text('SELECT * FROM game_play_by_play WHERE id = 15355363')).fetchone()
    print(f'Row 15355363: {row}')
