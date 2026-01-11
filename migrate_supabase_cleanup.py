
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def migrate_legacy_tables():
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    
    if not db_url:
        print("❌ SUPABASE_DB_URL not found.")
        return

    engine = create_engine(db_url, connect_args={"connect_timeout": 10})
    
    with engine.connect() as conn:
        print("🔌 Connected to Supabase...")
        
        # 1. Hitter Record
        try:
            print("🔄 Renaming hitter_record -> legacy_hitter_record...")
            conn.execute(text("ALTER TABLE IF EXISTS hitter_record RENAME TO legacy_hitter_record;"))
            print("✅ Hitter record renamed.")
        except Exception as e:
            print(f"⚠️ Hitter rename error: {e}")

        # 2. Pitcher Record
        try:
            print("🔄 Renaming pitcher_record -> legacy_pitcher_record...")
            conn.execute(text("ALTER TABLE IF EXISTS pitcher_record RENAME TO legacy_pitcher_record;"))
            print("✅ Pitcher record renamed.")
        except Exception as e:
            print(f"⚠️ Pitcher rename error: {e}")
            
        conn.commit()
        print("✨ Migration Complete.")

if __name__ == "__main__":
    migrate_legacy_tables()
