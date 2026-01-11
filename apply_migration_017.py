
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def apply_migration():
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("❌ SUPABASE_DB_URL not set")
        return

    print("🔌 Connecting to Supabase...")
    engine = create_engine(db_url)
    
    with open("migrations/supabase/017_create_team_daily_roster.sql", "r") as f:
        sql = f.read()
        
    with engine.connect() as conn:
        print("🚀 Applying migration 017 (team_daily_roster)...")
        conn.execute(text(sql))
        conn.commit()
        print("✅ Migration 017 applied successfully.")

if __name__ == "__main__":
    apply_migration()
