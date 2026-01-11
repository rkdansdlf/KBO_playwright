
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def inspect_supabase():
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    
    if not db_url:
        print("❌ SUPABASE_DB_URL or TARGET_DATABASE_URL not found in environment.")
        return

    try:
        engine = create_engine(db_url, connect_args={"connect_timeout": 10})
        with engine.connect() as conn:
            print(f"✅ Connected to Supabase!")
            
            # List tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name;
            """))
            tables = [row[0] for row in result]
            print(f"\n📋 Tables in 'public' schema ({len(tables)}):")
            for t in tables:
                print(f" - {t}")
            
            # Check row counts for key tables
            print("\n📊 Row Counts:")
            key_tables = ['game', 'game_batting_stats', 'player_basic', 'teams']
            for t in key_tables:
                if t in tables:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                    print(f" - {t}: {count}")
                else:
                    print(f" - {t}: [NOT FOUND]")

    except Exception as e:
        print(f"❌ Connection Failed: {e}")

if __name__ == "__main__":
    inspect_supabase()
