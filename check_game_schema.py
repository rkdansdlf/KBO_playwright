import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def check_game_schema():
    load_dotenv()
    db_url = os.getenv("TARGET_DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("❌ DATABASE URL not found.")
        return

    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("🔍 Inspecting 'game' table schema...")
            # Query information_schema
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'game'
                ORDER BY ordinal_position;
            """))
            
            columns = result.fetchall()
            
            if not columns:
                print("❌ Table 'game' does not exist.")
            else:
                print(f"✅ Table 'game' found with {len(columns)} columns:")
                print(f"{'Column Name':<20} | {'Type':<15} | {'Nullable'}")
                print("-" * 50)
                for col in columns:
                    print(f"{col[0]:<20} | {col[1]:<15} | {col[2]}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_game_schema()
