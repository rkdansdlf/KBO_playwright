import os
import sys
from pathlib import Path

# Ensure project root is in path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import text, create_engine
from src.db.engine import SessionLocal

def cleanup_local_db():
    print("🧹 Cleaning up incorrect fielding stats in local SQLite database...")
    with SessionLocal() as session:
        try:
            # Query count of rows to delete
            count_query = text("""
                SELECT COUNT(*) FROM player_season_fielding 
                WHERE (team_id = 'WO' AND year >= 2019) 
                   OR (team_id = 'SK' AND year >= 2021) 
                   OR (team_id = 'OB' AND year >= 1999) 
                   OR (team_id = 'HT' AND year >= 2001);
            """)
            rows_to_delete = session.execute(count_query).scalar()
            
            if rows_to_delete == 0:
                print("✅ No incorrect legacy team records found in local SQLite database.")
                return
                
            delete_query = text("""
                DELETE FROM player_season_fielding 
                WHERE (team_id = 'WO' AND year >= 2019) 
                   OR (team_id = 'SK' AND year >= 2021) 
                   OR (team_id = 'OB' AND year >= 1999) 
                   OR (team_id = 'HT' AND year >= 2001);
            """)
            result = session.execute(delete_query)
            session.commit()
            print(f"✅ Deleted {rows_to_delete} incorrect records from local SQLite player_season_fielding.")
        except Exception as e:
            session.rollback()
            print(f"❌ Error cleaning up local SQLite database: {e}")
            raise

def cleanup_oci_db():
    oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not oci_url:
        print("⚠️ OCI_DB_URL or TARGET_DATABASE_URL not set. Skipping OCI database cleanup.")
        return

    print("🧹 Cleaning up incorrect fielding stats in OCI PostgreSQL database...")
    try:
        oci_engine = create_engine(oci_url, pool_pre_ping=True)
        with oci_engine.connect() as conn:
            # Query count of rows to delete
            count_query = text("""
                SELECT COUNT(*) FROM player_season_fielding 
                WHERE (team_id = 'WO' AND year >= 2019) 
                   OR (team_id = 'SK' AND year >= 2021) 
                   OR (team_id = 'OB' AND year >= 1999) 
                   OR (team_id = 'HT' AND year >= 2001);
            """)
            rows_to_delete = conn.execute(count_query).scalar()
            
            if rows_to_delete == 0:
                print("✅ No incorrect legacy team records found in OCI PostgreSQL database.")
                return
                
            delete_query = text("""
                DELETE FROM player_season_fielding 
                WHERE (team_id = 'WO' AND year >= 2019) 
                   OR (team_id = 'SK' AND year >= 2021) 
                   OR (team_id = 'OB' AND year >= 1999) 
                   OR (team_id = 'HT' AND year >= 2001);
            """)
            conn.execute(delete_query)
            conn.commit()
            print(f"✅ Deleted {rows_to_delete} incorrect records from OCI PostgreSQL player_season_fielding.")
    except Exception as e:
        print(f"❌ Error cleaning up OCI PostgreSQL database: {e}")
        raise

if __name__ == "__main__":
    cleanup_local_db()
    cleanup_oci_db()
