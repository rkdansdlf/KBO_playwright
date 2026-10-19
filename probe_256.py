"""Read-only probe: pgvector capabilities + golden-query reference existence.

SELECT-only. No schema changes, no writes. Scratch script for the 256-dim
re-verification; delete after use.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import text  # noqa: E402

from src.db.vector_engine import get_vector_session  # noqa: E402

GOLDEN_PATH = Path("Docs/references/rag_golden_queries.json")


def main() -> int:
    url = os.environ.get("PGVECTOR_URL", "")
    print("PGVECTOR_URL:", re.sub(r"://[^@/]+@", "://***@", url))
    with get_vector_session() as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout = '120s'"))

        ext = session.execute(
            text("select extversion from pg_extension where extname = 'vector'")
        ).scalar()
        print("pgvector extversion:", ext)

        n = session.execute(text("select count(*) from rag_chunks")).scalar()
        print("rag_chunks total:", n)

        rows = session.execute(
            text(
                "select coalesce(index_status, '(null)') as st, count(*) "
                "from rag_chunks group by 1 order by 2 desc"
            )
        ).fetchall()
        print("index_status:", [(r[0], int(r[1])) for r in rows])

        dims = session.execute(
            text("select distinct vector_dims(embedding) from rag_chunks")
        ).fetchall()
        print("vector_dims distinct:", [r[0] for r in dims])

        nn = session.execute(
            text("select count(*) from rag_chunks where embedding is null")
        ).scalar()
        print("null embeddings:", nn)

        sub = session.execute(
            text(
                "select subvector(embedding, 1, 3)::text from rag_chunks "
                "where embedding is not null limit 1"
            )
        ).scalar()
        print("subvector sample:", sub)

        norm = session.execute(
            text(
                "select l2_normalize(subvector(embedding, 1, 3))::text from rag_chunks "
                "where embedding is not null limit 1"
            )
        ).scalar()
        print("l2_normalize sample:", norm)

        golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
        found = 0
        missing: list[str] = []
        for item in golden:
            for cid in item["relevantChunkIds"]:
                table, _, rid = cid.partition(":")
                hit = session.execute(
                    text(
                        "select 1 from rag_chunks "
                        "where source_table = :t and source_row_id = :r limit 1"
                    ),
                    {"t": table, "r": rid},
                ).scalar()
                if hit:
                    found += 1
                else:
                    missing.append(cid)
        print(f"golden refs: found={found} missing={len(missing)}")
        for cid in missing:
            print("  MISS", cid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
