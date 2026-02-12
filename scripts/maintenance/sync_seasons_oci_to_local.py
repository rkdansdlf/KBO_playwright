
import os
from sqlalchemy import create_engine, text
from src.db.engine import Engine

def sync_seasons():
    oci_url = os.getenv('OCI_DB_URL', 'postgresql://postgres:rkdansdlf@134.185.107.178:5432/postgres')
    oci_engine = create_engine(oci_url)
    
    # 1. Fetch from OCI
    with oci_engine.connect() as oci_conn:
        print("📡 Fetching kbo_seasons from OCI...")
        result = oci_conn.execute(text("SELECT season_id, season_year, league_type_code, league_type_name, start_date, end_date FROM kbo_seasons"))
        seasons = [dict(r._mapping) for r in result]
        print(f"✅ Found {len(seasons)} seasons in OCI.")
        
    # 2. Save to Local
    with Engine.connect() as local_conn:
        print("💾 Saving kbo_seasons to local SQLite...")
        # Clear local first (optional but safer for clean sync)
        local_conn.execute(text("DELETE FROM kbo_seasons"))
        
        for s in seasons:
            # Handle dates (Postgres Date to SQLite string/Date)
            s['start_date'] = str(s['start_date']) if s['start_date'] else None
            s['end_date'] = str(s['end_date']) if s['end_date'] else None
            
            local_conn.execute(text("""
                INSERT OR REPLACE INTO kbo_seasons (season_id, season_year, league_type_code, league_type_name, start_date, end_date, created_at, updated_at)
                VALUES (:season_id, :season_year, :league_type_code, :league_type_name, :start_date, :end_date, DATETIME('now'), DATETIME('now'))
            """), s)
        
        local_conn.commit()
        print(f"✅ Synced {len(seasons)} seasons to local DB.")

if __name__ == "__main__":
    sync_seasons()
