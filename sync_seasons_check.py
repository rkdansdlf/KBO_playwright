
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), "."))

from src.db.engine import SessionLocal
from src.models.season import KboSeason

def sync_seasons():
    load_dotenv()
    
    # Local
    sqlite_session = SessionLocal()
    
    # Remote
    supabase_url = os.getenv("SUPABASE_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not supabase_url:
        print("❌ Missing SUPABASE_DB_URL")
        return

    pg_engine = create_engine(supabase_url)
    
    # Read all local
    seasons = sqlite_session.query(KboSeason).all()
    print(f"Loaded {len(seasons)} seasons from SQLite.")
    
    # Sync to PG
    with pg_engine.connect() as conn:
        for s in seasons:
            # UPSERT
            stmt = text("""
                INSERT INTO kbo_seasons (
                    season_id, season_year, league_type_code, league_type_name, 
                    start_date, end_date, created_at, updated_at
                ) VALUES (
                    :season_id, :year, :league_type, :league_name, 
                    :start_date, :end_date, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (season_id) DO NOTHING;
            """)
            
            conn.execute(stmt, {
                "season_id": s.season_id,
                "year": s.season_year,
                "league_type": s.league_type_code,
                "league_name": s.league_type_name,
                "start_date": s.start_date,
                "end_date": s.end_date
            })
        conn.commit()
    
    print("✅ Synced seasons to Supabase.")

if __name__ == "__main__":
    sync_seasons()
