import os
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def verify_supabase():
    load_dotenv()
    db_url = os.getenv("TARGET_DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("❌ DB URL not set")
        return

    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("\n📊 Supabase Data Integrity Check\n" + "="*40)
            
            tables = [
                'game', 'game_metadata', 'game_inning_scores', 
                'game_lineups', 'game_batting_stats', 'game_pitching_stats', 'game_events'
            ]
            
            for table in tables:
                # 1. Count rows
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                print(f"\n🔹 Table: {table} (Rows: {count})")
                
                if count == 0:
                    print("   ⚠️  Empty table")
                    continue
                
                # 2. Check for NULLs in common important columns if they exist
                cols_to_check = ['game_id', 'team_code', 'player_name', 'runs', 'hits']
                # Get actual columns first
                actual_cols = [row[0] for row in conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"))]
                
                for col in cols_to_check:
                    if col in actual_cols:
                        null_count = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")).scalar()
                        if null_count > 0:
                            print(f"   ❌ Column '{col}' has {null_count} NULL values!")
                        else:
                            print(f"   ✅ Column '{col}' OK (No NULLs)")

                # 3. Sample Date
                if count > 0:
                    sample = conn.execute(text(f"SELECT * FROM {table} LIMIT 1")).mappings().first()
                    print(f"   👀 Sample Row: {dict(sample)}")

    except Exception as e:
        print(f"\n❌ Verification Error: {e}")

if __name__ == "__main__":
    verify_supabase()
