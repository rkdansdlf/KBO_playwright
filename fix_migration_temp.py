import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def fix_migration():
    url = os.getenv('SUPABASE_DB_URL')
    if not url:
        print("❌ SUPABASE_DB_URL not found")
        sys.exit(1)
    
    engine = create_engine(url)
    
    # Drop the old index first and create a new one that handles NULLs better
    # Postgres UNIQUE constraints/indexes consider NULLs as distinct values.
    # To use NULL-able columns in ON CONFLICT, we should use COALESCE or similar in the index.
    
    sql = """
    DROP INDEX IF EXISTS public.uq_game_summary_entry;
    
    CREATE UNIQUE INDEX uq_game_summary_entry 
    ON public.game_summary (
        game_id, 
        COALESCE(summary_type, ''), 
        COALESCE(player_name, ''), 
        MD5(detail_text)
    );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
            print("✅ Successfully updated uq_game_summary_entry index in Supabase")
    except Exception as e:
        print(f"❌ Error updating migration: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fix_migration()
