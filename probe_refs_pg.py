"""Read-only: golden-ref status check via raw psycopg2 (diag_conn pattern).

Distinguishes DELETED / other status / no-row for every golden reference, and
censors the game_highlights table by status.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import urlsplit

import psycopg2
from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def normalize(url: str) -> str:
    url = url.replace("postgresql+psycopg2://", "postgresql://")
    parts = urlsplit(url)
    return f"postgresql://{parts.netloc}{parts.path}"


print("step 1: loading golden refs", flush=True)
golden = json.loads(Path("Docs/references/rag_golden_queries.json").read_text(encoding="utf-8"))
refs = sorted({cid for item in golden for cid in item["relevantChunkIds"]})
print(f"step 2: {len(refs)} distinct refs; connecting", flush=True)

conn = psycopg2.connect(normalize(__import__("os").environ["PGVECTOR_URL"]), connect_timeout=15)
conn.set_session(readonly=True)
cur = conn.cursor()
print("step 3: connected (read-only)", flush=True)

cur.execute(
    "select source_table || ':' || source_row_id as cid, coalesce(index_status, '(null)') "
    "from rag_chunks where source_table || ':' || source_row_id = any(%s)",
    (refs,),
)
found = dict(cur.fetchall())
print(f"step 4: {len(found)}/{len(refs)} refs have a row", flush=True)
for cid in refs:
    print(f"    {cid:<32} -> {found.get(cid, '(NO ROW)')}", flush=True)

cur.execute(
    "select coalesce(index_status, '(null)') as st, count(*) as n "
    "from rag_chunks where source_table = 'game_highlights' group by 1 order by 2 desc"
)
print("step 5: game_highlights census by status:", cur.fetchall(), flush=True)

cur.execute(
    "select count(*) from rag_chunks where source_table || ':' || source_row_id = any(%s) "
    "and (index_status is null or index_status in ('ACTIVE','INDEXED'))",
    (refs,),
)
print("step 6: retrievable golden refs:", cur.fetchone()[0], flush=True)

conn.close()
print("done", flush=True)
