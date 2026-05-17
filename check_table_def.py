from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name, data_type, column_default, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'game_play_by_play'
        ORDER BY ordinal_position;
    """))
    for row in result:
        print(row)
