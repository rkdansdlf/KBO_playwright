from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Date, Boolean, Time, Text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import time
import json
import sys

def debug_pg_insert():
    url = 'postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend'
    engine = create_engine(url)
    metadata = MetaData()
    
    # Define table exactly as in SupabaseSync
    game_metadata_table = Table('game_metadata', metadata, schema='public', autoload_with=engine)
    
    # Exact failing data
    data = {
        'game_id': 'DEBUG_FAIL_CASE', # Changed ID to avoid PK conflict with existing
        'stadium_code': None,
        'stadium_name': '무등',
        'attendance': 1787,
        'start_time': time(18, 30),
        'end_time': time(22, 6),
        'game_time_minutes': 216,
        'weather': None,
        # Pass raw dict, NOT json.dumps string, to see if SQLAlchemy/Psycopg2 fails
        'source_payload': {'stadium': '무등', 'attendance': 1787, 'start_time': '18:30', 'end_time': '22:06', 'game_time': '3:36', 'duration_minutes': 216}
    }
    
    print(f"Testing pg_insert with data: {data}")
    
    with engine.connect() as conn:
        try:
            stmt = pg_insert(game_metadata_table).values(**data)
            conn.execute(stmt)
            conn.commit()
            print("✅ Success with pg_insert")
        except Exception as e:
            print(f"❌ Failed with pg_insert: {e}")

if __name__ == "__main__":
    debug_pg_insert()
