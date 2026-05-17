from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
with engine.connect() as conn:
    result = conn.execute(text("SELECT game_id, COUNT(*) FROM game GROUP BY game_id HAVING COUNT(*) > 1"))
    dupes = result.fetchall()
    print(f"Duplicate game_ids in OCI: {len(dupes)}")
    for d in dupes[:10]:
        print(d)

    # Check for games in OCI not in Local
    # I don't have local DB in this script easily without more setup, but I can check total count of unique game_ids
    unique_count = conn.execute(text("SELECT COUNT(DISTINCT game_id) FROM game")).scalar()
    print(f"Unique game_ids in OCI: {unique_count}")
