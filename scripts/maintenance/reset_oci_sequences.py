
import os
import sys
from sqlalchemy import text
from src.db.engine import create_engine_for_url
from dotenv import load_dotenv

def reset_sequences(target_url=None):
    if not target_url:
        load_dotenv()
        target_url = os.getenv("OCI_DB_URL")
    if not target_url:
        print("❌ OCI_DB_URL not found")
        return

    print(f"🔄 Resetting sequences on {target_url.split('@')[-1]}...")
    engine = create_engine_for_url(target_url)
    
    # Query to find all sequences and their associated tables/columns
    sql = """
    SELECT 
        'SELECT setval(' || quote_literal(quote_ident(s.relname)) || ', COALESCE(MAX(' || quote_ident(c.attname) || '), 1)) FROM ' || quote_ident(t.relname) || ';' as cmd
    FROM pg_class s
    JOIN pg_depend d ON d.objid = s.oid
    JOIN pg_attribute c ON c.attrelid = d.refobjid AND c.attnum = d.refobjsubid
    JOIN pg_class t ON t.oid = d.refobjid
    WHERE s.relkind = 'S'
    AND d.deptype = 'a';
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            cmds = [row[0] for row in result]
            
            for cmd in cmds:
                print(f"  Exectuing: {cmd}")
                conn.execute(text(cmd))
            
            conn.commit()
            print("✅ All sequences reset successfully")
    except Exception as e:
        print(f"❌ Failed to reset sequences: {e}")

if __name__ == "__main__":
    reset_sequences()
