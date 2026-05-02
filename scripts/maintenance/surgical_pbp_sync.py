"""
Surgical PBP Sync: Forcefully sync game_play_by_play for specific years.
Purpose: Solve the discrepancy where PBP counts are stuck in OCI.
"""
import os
import sys
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.db.engine import SessionLocal as LocalSession
from src.models.game import GamePlayByPlay

load_dotenv()

def surgical_sync(years):
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL not found.")
        return

    print(f"🚀 Initializing Surgical PBP Sync for years: {years}...")
    oci_engine = create_engine(oci_url)
    OCI_Session = sessionmaker(bind=oci_engine)
    
    with OCI_Session() as oci_session, LocalSession() as local_session:
        for year in years:
            pattern = f"{year}%"
            print(f"\n📅 Processing year: {year}")
            
            # 1. Count Local Rows
            local_count = local_session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id.like(pattern)).count()
            print(f"   Local PBP Rows: {local_count}")
            
            if local_count == 0:
                print(f"   ⚠️ No PBP data found in Local for {year}. Skipping.")
                continue

            # 2. Delete OCI Rows for this year
            print(f"   🧹 Deleting existing PBP rows in OCI for {year}...")
            deleted = oci_session.execute(
                text("DELETE FROM game_play_by_play WHERE game_id LIKE :pattern"),
                {"pattern": pattern}
            )
            oci_session.commit()
            print(f"   ✅ Deleted {deleted.rowcount} rows in OCI.")

            # 3. Fetch Local Data in Chunks and Insert
            print(f"   🚚 Transferring {local_count} rows from Local to OCI...")
            chunk_size = 5000
            synced = 0
            
            # Get all column names except 'id'
            columns = [c.key for c in GamePlayByPlay.__table__.columns if c.key != 'id']
            
            for offset in range(0, local_count, chunk_size):
                rows = local_session.query(GamePlayByPlay).filter(
                    GamePlayByPlay.game_id.like(pattern)
                ).offset(offset).limit(chunk_size).all()
                
                mappings = []
                for r in rows:
                    mappings.append({col: getattr(r, col) for col in columns})
                
                if mappings:
                    # Use core insert for speed and to skip model instance overhead
                    oci_session.execute(GamePlayByPlay.__table__.insert(), mappings)
                    oci_session.commit()
                    synced += len(mappings)
                    print(f"      Synced {synced}/{local_count}...")

            print(f"   ✨ Year {year} sync complete. Total: {synced}")

    print("\n✅ Surgical Sync Finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", type=int, default=[2025, 2024])
    args = parser.parse_args()
    
    surgical_sync(args.years)
