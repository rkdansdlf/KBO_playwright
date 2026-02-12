
import os
from sqlalchemy import create_engine, text
from src.db.engine import Engine

def diagnose_1982():
    local_url = "sqlite:///data/kbo_dev.db" # Or use Engine
    oci_url = os.getenv('OCI_DB_URL', 'postgresql://postgres:rkdansdlf@134.185.107.178:5432/bega_backend')
    
    oci_engine = create_engine(oci_url)
    
    with Engine.connect() as local_conn:
        local_ids = set(r[0] for r in local_conn.execute(text("SELECT player_id FROM player_season_batting WHERE season=1982")).fetchall())
        print(f"Local 1982 Batters: {len(local_ids)}")
        
    with oci_engine.connect() as oci_conn:
        oci_ids = set(r[0] for r in oci_conn.execute(text("SELECT player_id FROM player_season_batting WHERE season=1982")).fetchall())
        print(f"OCI 1982 Batters: {len(oci_ids)}")
        
        missing = local_ids - oci_ids
        print(f"Missing IDs in OCI: {len(missing)}")
        
        if missing:
            sample_id = list(missing)[0]
            print(f"Sample Missing ID: {sample_id}")
            
            # Check if this player exists in OCI player_basic
            exists = oci_conn.execute(text(f"SELECT COUNT(*) FROM player_basic WHERE player_id={sample_id}")).scalar()
            print(f"Sample ID exists in OCI player_basic? {exists}")
            
            # Check constraints
            print("--- OCI player_season_batting Constraints ---")
            sql = """
                SELECT conname, pg_get_constraintdef(c.oid)
                FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
                WHERE c.conrelid = 'public.player_season_batting'::regclass
            """
            for r in oci_conn.execute(text(sql)).fetchall():
                print(r)

if __name__ == "__main__":
    diagnose_1982()
