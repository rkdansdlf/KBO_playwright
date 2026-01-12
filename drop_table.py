import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def drop_table():
    load_dotenv()
    target_url = os.getenv("TARGET_DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not target_url:
        print("No Target URL found")
        return

    engine = create_engine(target_url)
    with engine.connect() as conn:
        print("Dropping player_basic table on remote...")
        conn.execute(text("DROP TABLE IF EXISTS player_basic CASCADE"))
        conn.commit()
        print("Dropped.")

if __name__ == "__main__":
    drop_table()
