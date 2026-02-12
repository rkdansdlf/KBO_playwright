
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

def migrate_oci_schema():
    load_dotenv()
    oci_url = os.getenv('OCI_DB_URL')
    if not oci_url:
        print("❌ OCI_DB_URL environment variable not set")
        return

    print(f"🔌 Connecting to OCI: {oci_url.split('@')[-1]}") # Hide credentials
    
    try:
        engine = create_engine(oci_url)
        with engine.connect() as conn:
            tables = ['game_batting_stats', 'game_pitching_stats', 'game_lineups']
            
            for table in tables:
                print(f"\nChecking table: {table}")
                try:
                    # Check if column exists
                    check_sql = text(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = :table AND column_name = 'uniform_no'
                    """)
                    result = conn.execute(check_sql, {'table': table}).fetchone()
                    
                    if result:
                        print(f"  ✅ Column 'uniform_no' already exists.")
                    else:
                        print(f"  ✨ Adding 'uniform_no' column...")
                        alter_sql = text(f"ALTER TABLE {table} ADD COLUMN uniform_no VARCHAR(10)")
                        conn.execute(alter_sql)
                        conn.commit()
                        print(f"  ✅ Successfully added 'uniform_no'.")
                        
                except Exception as e:
                    print(f"  ❌ Error processing {table}: {e}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    migrate_oci_schema()
