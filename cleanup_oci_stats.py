import sqlite3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
local_conn = sqlite3.connect('data/kbo_dev.db')

def cleanup_table(oci_conn, table_name, unique_cols):
    local_keys = set(local_conn.execute(f"SELECT {','.join(unique_cols)} FROM {table_name}"))
    
    oci_rows = oci_conn.execute(text(f"SELECT id, {','.join(unique_cols)} FROM {table_name}")).fetchall()
    oci_keys = {tuple(row[1:]): row[0] for row in oci_rows}
    
    to_delete = []
    for key, row_id in oci_keys.items():
        if key not in local_keys:
            to_delete.append(row_id)
            
    print(f"Table {table_name}: {len(to_delete)} rows to delete from OCI.")
    if to_delete:
        batch_size = 500
        for i in range(0, len(to_delete), batch_size):
            batch = to_delete[i:i+batch_size]
            oci_conn.execute(text(f"DELETE FROM {table_name} WHERE id IN :ids"), {"ids": tuple(batch)})
            oci_conn.commit()
            print(f"  Deleted {i + len(batch)}/{len(to_delete)} rows...")

oci_engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
with oci_engine.connect() as conn:
    unique_cols = ['player_id', 'season', 'league', 'level']
    cleanup_table(conn, 'player_season_batting', unique_cols)
    cleanup_table(conn, 'player_season_pitching', unique_cols)

print("Cleanup complete.")
