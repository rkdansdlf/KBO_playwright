import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def check_tables():
    load_dotenv()
    db_url = os.getenv("TARGET_DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("❌ TARGET_DATABASE_URL or SUPABASE_DB_URL is not set.")
        return

    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            required_tables = [
                "game", 
                "game_metadata", 
                "game_inning_scores", 
                "game_lineups", 
                "game_batting_stats", 
                "game_pitching_stats", 
                "game_events"
            ]
            
            missing = []
            print(f"🔍 Checking for existence of {len(required_tables)} tables in Supabase...")
            
            for table in required_tables:
                try:
                    # Lightweight check
                    conn.execute(text(f"SELECT 1 FROM {table} LIMIT 0"))
                    print(f"  ✅ {table} exists")
                except Exception as e:
                    print(f"  ❌ {table} NOT FOUND (or error: {e})")
                    missing.append(table)
            
            if missing:
                print("\n⚠️  The following tables are MISSING. Please run 011_expand_game_detail.sql:")
                for t in missing:
                    print(f"  - {t}")
                exit(1)
            else:
                print("\n✨ All required tables exist! You can proceed with sync.")
                exit(0)

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        exit(1)

if __name__ == "__main__":
    check_tables()
