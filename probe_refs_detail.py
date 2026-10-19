"""Read-only: distinguish DELETED vs missing rows for golden refs; source_table census."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import text  # noqa: E402

from src.db.vector_engine import get_vector_session  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

golden = json.loads(Path("Docs/references/rag_golden_queries.json").read_text(encoding="utf-8"))
refs = sorted({cid for item in golden for cid in item["relevantChunkIds"]})

with get_vector_session() as session:
    session.execute(text("SET TRANSACTION READ ONLY"))
    session.execute(text("SET LOCAL statement_timeout = '120s'"))

    rows = session.execute(
        text(
            "select source_table || ':' || source_row_id as cid, "
            "coalesce(index_status, '(null)') as st "
            "from rag_chunks "
            "where source_table || ':' || source_row_id = any(cast(:refs as text[]))"
        ),
        {"refs": refs},
    ).fetchall()
    found = {r[0]: r[1] for r in rows}
    print(f"refs total={len(refs)} rows_found={len(found)}")
    missing = [cid for cid in refs if cid not in found]
    print(f"refs with NO row in store: {missing or 'none'}")
    bad = [(cid, st) for cid, st in sorted(found.items()) if st not in ("ACTIVE", "INDEXED")]
    print(f"refs present but non-retrievable: {bad or 'none'}")

    census = session.execute(
        text(
            "select source_table, count(*) filter (where index_status in ('ACTIVE','INDEXED')) as retrievable, "
            "count(*) as total from rag_chunks where source_table = any(cast(:tables as text[])) "
            "group by source_table order by source_table"
        ),
        {
            "tables": sorted(
                {
                    cid.split(":", 1)[0]
                    for cid in refs
                }
            )
        },
    ).fetchall()
    print("source_table census for golden tables:")
    for table, retr, total in census:
        print(f"  {table:<28} retrievable={int(retr):>7} total={int(total):>7}")

    # prod_rank_013 / prod_highlight_027 details
    for item in golden:
        if item["id"] in ("prod_rank_013", "prod_highlight_027", "prod_rule_020"):
            cids = item["relevantChunkIds"]
            print(f"{item['id']}: query={item['query']!r} filters={item.get('filters')} refs={cids}")
            for cid in cids:
                st = found.get(cid, "(no row)")
                print(f"    {cid} -> {st}")
