
import os
from sqlalchemy import create_engine, text

def fix_oci_player_basic():
    oci_url = os.getenv('OCI_DB_URL', 'postgresql://postgres:rkdansdlf@134.185.107.178:5432/postgres')
    engine = create_engine(oci_url)
    
    with engine.connect() as conn:
        print("🔍 Checking duplicates before cleanup...")
        count = conn.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()
        print(f"Total rows: {count}")
        
        distinct_count = conn.execute(text("SELECT COUNT(DISTINCT player_id) FROM player_basic")).scalar()
        print(f"Distinct player_ids: {distinct_count}")
        
        if count > distinct_count:
            print(f"⚠️ Found {count - distinct_count} duplicate rows. Deduplicating...")
            
            # Deduplication: Keep the row with the largest ctid (latest insert)
            conn.execute(text("""
                DELETE FROM player_basic a 
                USING player_basic b
                WHERE a.player_id = b.player_id AND a.ctid < b.ctid
            """))
            conn.commit()
            print("✅ Deduplication complete.")
        else:
            print("✅ No duplicates found.")
            
        # Add Primary Key
        print("🔧 Adding PRIMARY KEY to player_basic(player_id)...")
        try:
            conn.execute(text("ALTER TABLE player_basic ADD PRIMARY KEY (player_id)"))
            conn.commit()
            print("✅ Primary Key added successfully.")
        except Exception as e:
            print(f"⚠️  Could not add PK (might already exist or other error): {e}")

if __name__ == "__main__":
    fix_oci_player_basic()
