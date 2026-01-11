
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
    
    with open("migrations/supabase/016_create_rank_view.sql", "r") as f:
        sql = f.read()
        
    with engine.connect() as conn:
        print("🚀 Applying migration 016 (v_team_rank_all)...")
        conn.execute(text(sql))
        conn.commit()
        print("✅ Ranking View created successfully.")

if __name__ == "__main__":
    apply_migration()
