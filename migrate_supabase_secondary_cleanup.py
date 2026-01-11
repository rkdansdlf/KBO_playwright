
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def migrate_secondary_tables():
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    
    if not db_url:
        print("❌ SUPABASE_DB_URL not found.")
        return

    engine = create_engine(db_url, connect_args={"connect_timeout": 10})
    
    with engine.connect() as conn:
        print("🔌 Connected to Supabase...")
        
        tables_to_rename = [
            ("stadiums", "legacy_stadiums"),
            ("ranking_predictions", "legacy_ranking_predictions"),
            ("predictions", "legacy_predictions"),
        ]
        
        for old_name, new_name in tables_to_rename:
            try:
                print(f"🔄 Renaming {old_name} -> {new_name}...")
                conn.execute(text(f"ALTER TABLE IF EXISTS {old_name} RENAME TO {new_name};"))
                print(f"✅ {old_name} renamed.")
            except Exception as e:
                print(f"⚠️ Rename error: {e}")
            
        conn.commit()
        print("✨ Secondary Cleanup Complete.")

if __name__ == "__main__":
    migrate_secondary_tables()
