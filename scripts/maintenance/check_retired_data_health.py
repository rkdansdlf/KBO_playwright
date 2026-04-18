
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

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
    
    print("\n📊 KBO Retired Data Health Check")
    print("-" * 60)
    
    tables = [
        ("player_basic", "Basic Search Data"),
        ("players", "Master Player Records"),
        ("player_season_batting", "Batting Stats"),
        ("player_season_pitching", "Pitching Stats")
    ]
    
    for table_name, desc in tables:
        # Local count
        local_count = local_session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        
        # OCI count
        try:
            with oci_engine.connect() as conn:
                oci_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        except Exception as e:
            oci_count = f"Error"
            
        print(f"{desc:<22} | Local: {local_count:<6} | OCI: {oci_count:<6}")
    
    print("-" * 60)
        
    # Check for photo_url and retirement status in Master Records
    retired_with_photo = local_session.execute(
        text("SELECT COUNT(*) FROM players WHERE status='RETIRED' AND (photo_url IS NOT NULL AND photo_url != '')")
    ).scalar()
    total_retired = local_session.execute(
        text("SELECT COUNT(*) FROM players WHERE status='RETIRED'")
    ).scalar()
    
    print(f"📸 Master Records: Retired players with photo: {retired_with_photo} / {total_retired}")
    
    # 1. Check for missing master records
    missing_count = local_session.execute(text("""
        SELECT COUNT(*) FROM player_basic pb 
        LEFT JOIN players p ON pb.player_id = CAST(p.kbo_person_id AS INTEGER)
        WHERE p.id IS NULL
    """)).scalar()
    
    # 2. Check for status mismatches
    mismatch_count = local_session.execute(text("""
        SELECT COUNT(*) FROM player_basic pb 
        JOIN players p ON pb.player_id = CAST(p.kbo_person_id AS INTEGER)
        WHERE lower(pb.status) != lower(p.status)
        AND pb.status IS NOT NULL
    """)).scalar()

    print(f"⚠️  Missing Master Records (Search -> Master): {missing_count}")
    print(f"🔄 Status Mismatches (Basic vs Master): {mismatch_count}")
    print("-" * 60)
    
    local_session.close()

if __name__ == "__main__":
    check_health()
