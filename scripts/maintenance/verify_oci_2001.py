
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load env variables
load_dotenv()

def verify_oci_2001():
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL not found in environment.")
        return

    print(f"🔌 Connecting to OCI DB: {oci_url.split('@')[1] if '@' in oci_url else '***'}")
    
    try:
        engine = create_engine(oci_url)
        with engine.connect() as conn:
            # 1. Check Game Count
            result = conn.execute(text("SELECT COUNT(*) FROM game WHERE game_id LIKE '2001%'"))
            game_count = result.scalar()
            print(f"📊 Game Count (2001%): {game_count}")
            
            # 2. Check Batting Stats Count
            result = conn.execute(text("SELECT COUNT(*) FROM game_batting_stats WHERE game_id LIKE '2001%'"))
            batting_count = result.scalar()
            print(f"📊 Batting Stats Count (2001%): {batting_count}")
            
            # 3. Check Season ID sample if games exist
            if game_count > 0:
                result = conn.execute(text("SELECT game_id, season_id FROM game WHERE game_id LIKE '2001%' LIMIT 5"))
                print("🔍 Sample 2001 Games:")
                for row in result:
                    print(f"   - {row[0]}: season_id={row[1]}")
            else:
                print("⚠️ No games found for 2001.")
                
            # 4. Check if we have ANY data
            result = conn.execute(text("SELECT COUNT(*) FROM game"))
            total_games = result.scalar()
            print(f"ℹ️  Total Games in DB: {total_games}")
            
            # 5. Check 2024 sample for season_id convention
            print("\n🔍 Sample 2024 Games:")
            result = conn.execute(text("SELECT game_id, season_id FROM game WHERE game_id LIKE '2024%' LIMIT 5"))
            for row in result:
                print(f"   - {row[0]}: season_id={row[1]}")
            
    except Exception as e:
        print(f"❌ Error connecting/querying OCI: {e}")

if __name__ == "__main__":
    verify_oci_2001()
