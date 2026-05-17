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

print(f"Local games: {len(local_game_ids)}")
print(f"OCI games: {len(oci_game_ids)}")

only_local = local_game_ids - oci_game_ids
only_oci = oci_game_ids - local_game_ids

print(f"Games ONLY in Local: {len(only_local)}")
if only_local:
    print(f"Sample: {list(only_local)[:5]}")

print(f"Games ONLY in OCI: {len(only_oci)}")
if only_oci:
    print(f"Sample: {list(only_oci)[:5]}")
