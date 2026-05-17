import sqlite3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
local_conn = sqlite3.connect('data/kbo_dev.db')
local_game_ids = set(row[0] for row in local_conn.execute("SELECT game_id FROM game"))

oci_engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
with oci_engine.connect() as conn:
    oci_game_ids = set(row[0] for row in conn.execute(text("SELECT game_id FROM game")))
    
    only_oci = oci_game_ids - local_game_ids
    print(f"Games to delete from OCI: {len(only_oci)}")
    
    if only_oci:
        # Delete in batches to avoid locking issues or long transactions
        only_oci_list = list(only_oci)
        batch_size = 100
        for i in range(0, len(only_oci_list), batch_size):
            batch = only_oci_list[i:i+batch_size]
            conn.execute(text("DELETE FROM game WHERE game_id IN :ids"), {"ids": tuple(batch)})
            conn.commit()
            print(f"  Deleted {i + len(batch)}/{len(only_oci_list)} games...")

    print("Cleanup complete.")
