
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def check_oci_stats():
    load_dotenv()
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("❌ OCI_DB_URL not found in environment")
        return

    engine = create_engine(oci_url)
    
    tables_to_check = [
        ("player_season_fielding", "year"),
        ("player_season_baserunning", "year"),
        ("team_season_batting", "season"),
        ("team_season_pitching", "season")
    ]
    
    print("Checking OCI Data for Year 2026...")
    print("-" * 40)
    
    try:
        with engine.connect() as conn:
            for table, year_col in tables_to_check:
                query = text(f'SELECT COUNT(*) FROM "{table}" WHERE "{year_col}" = 2026')
                count = conn.execute(query).scalar()
                print(f"✅ {table:26} : {count} rows")
                
                if count > 0:
                    # Show a few samples
                    sample_query = text(f'SELECT * FROM "{table}" WHERE "{year_col}" = 2026 LIMIT 1')
                    sample = conn.execute(sample_query).fetchone()
                    # print(f"   Sample: {sample}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_oci_stats()
