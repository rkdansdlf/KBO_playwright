import sqlite3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
local_conn = sqlite3.connect('data/kbo_dev.db')
local_counts = {}
for row in local_conn.execute("SELECT substr(game_id, 1, 4) as year, count(*) FROM game_summary GROUP BY year"):
    local_counts[row[0]] = row[1]

oci_engine = create_engine(os.getenv('TARGET_DATABASE_URL'))
oci_counts = {}
with oci_engine.connect() as conn:
    for row in conn.execute(text("SELECT substr(game_id, 1, 4) as year, count(*) FROM game_summary GROUP BY year")):
        oci_counts[row[0]] = row[1]

years = sorted(set(local_counts.keys()) | set(oci_counts.keys()))
print("Year | Local Sum | OCI Sum | Diff")
for y in years:
    l = local_counts.get(y, 0)
    o = oci_counts.get(y, 0)
    print(f"{y} | {l:9} | {o:7} | {o-l:5}")
