import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

def apply_migration(file_path: str):
    load_dotenv()
    oci_url = os.getenv('OCI_DB_URL')
    if not oci_url:
        print("❌ OCI_DB_URL environment variable not set")
        return

    if not os.path.exists(file_path):
        print(f"❌ Migration file not found: {file_path}")
        return

    print(f"🔌 Connecting to OCI for migration: {file_path}")
    
    try:
        engine = create_engine(oci_url)
        with engine.connect() as conn:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql = f.read()
            
            # Split by semicolon for execution if needed, but for simple ALTERs, single execute works
            print(f"📜 Executing SQL from {file_path}...")
            conn.execute(text(sql))
            conn.commit()
            print("✅ Migration applied successfully.")

    except Exception as e:
        print(f"❌ Migration failed: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="SQL migration file path")
    args = parser.parse_args()
    apply_migration(args.file)
