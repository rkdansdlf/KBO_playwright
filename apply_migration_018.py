
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), "."))

# Load Env
load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("TARGET_DATABASE_URL")

if not SUPABASE_DB_URL:
    print("❌ SUPABASE_DB_URL or TARGET_DATABASE_URL is missing.")
    sys.exit(1)

def apply_migration():
    print("🚀 Applying Migration 018: Create Player Movements table...")
    
    engine = create_engine(SUPABASE_DB_URL)
    
    migration_file = "migrations/supabase/018_create_player_movements.sql"
    
    try:
        with open(migration_file, "r") as f:
            sql = f.read()
            
        with engine.connect() as conn:
            # Split by statement if needed, or execute as block
            # For transaction safety, let's keep it robust
            with conn.begin():
                conn.execute(text(sql))
            
        print("✅ Migration applied successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    apply_migration()
