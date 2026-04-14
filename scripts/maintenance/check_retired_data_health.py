
import sys
import os
from sqlalchemy import text
from src.db.engine import SessionLocal
from src.cli.sync_oci import create_engine_for_url
from dotenv import load_dotenv

def check_health():
    load_dotenv()
    local_session = SessionLocal()
    
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL not found in .env")
        return

    oci_engine = create_engine_for_url(oci_url)
    
    print("📊 KBO Retired Data Health Check")
    print("-" * 40)
    
    tables = ["player_basic", "player_season_batting", "player_season_pitching"]
    
    for table in tables:
        # Local count
        local_count = local_session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        
        # OCI count
        try:
            with oci_engine.connect() as conn:
                oci_count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        except Exception as e:
            oci_count = f"Error ({e})"
            
        print(f"Table: {table}")
        print(f"  Local: {local_count}")
        print(f"  OCI:   {oci_count}")
        
    # Check for photo_url presence
    retired_with_photo = local_session.execute(
        text("SELECT COUNT(*) FROM player_basic WHERE status='RETIRED' AND photo_url IS NOT NULL")
    ).scalar()
    total_retired = local_session.execute(
        text("SELECT COUNT(*) FROM player_basic WHERE status='RETIRED'")
    ).scalar()
    
    print("-" * 40)
    print(f"📸 Retired players with photo: {retired_with_photo} / {total_retired}")
    
    local_session.close()

if __name__ == "__main__":
    check_health()
