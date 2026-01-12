import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def list_tables():
    load_dotenv()
    target_url = os.getenv("TARGET_DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not target_url:
        print("No Target URL found")
        return

    engine = create_engine(target_url)
    with engine.connect() as conn:
        print("Fetching table list from Supabase...")
        # Query to get all table names in public schema
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name;
        """))
        
        tables = [row[0] for row in result]
        
        print(f"\nFound {len(tables)} tables:")
        print("-" * 40)
        print(f"{'Table Name':<30} | {'Row Count':<10}")
        print("-" * 40)
        
        for tbl in tables:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
                print(f"{tbl:<30} | {count:<10}")
            except Exception as e:
                print(f"{tbl:<30} | Error: {e}")

if __name__ == "__main__":
    list_tables()
