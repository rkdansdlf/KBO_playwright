"""Read-only verification: does truncating pplx-1536 embeddings to 256 dims preserve dense retrieval?

Method
------
- Golden queries: Docs/references/rag_golden_queries.json (30 queries, 40 relevant refs).
- Query embeddings fetched once via EmbeddingService(cache_enabled=False) -> no
  embedding_cache reads/writes.
- Corpus snapshot frozen with REPEATABLE READ READ ONLY: every dimension sees the
  exact same 235,902 rows.
- For each dim in (1536, 1024, 512, 256, 128):
    * candidate expression mirrors production `search_by_cosine` semantics
      (status IN (NULL|ACTIVE|INDEXED), embedding IS NOT NULL, golden filters);
    * stored vector is truncated in-database via subvector(embedding, 1, N) -- the
      same MRL truncation a 256 migration would apply;
    * query vector is truncated locally (v[:N]) -- cosine is scale-invariant, so
      this is equivalent to MRL-N + renormalize for ranking purposes;
    * enable_indexscan=off forces exact scans for every dim (no ANN-vs-exact bias).
- Metrics per dim: Recall@5, MRR, hit@5, top-1; per-query deltas vs 1536.

Writes nothing. Scratch script for the dimension decision; delete after use.
"""

from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import text  # noqa: E402

from src.db.vector_engine import get_vector_session  # noqa: E402
from src.services.embedding_service import EmbeddingService  # noqa: E402

GOLDEN_PATH = Path("Docs/references/rag_golden_queries.json")
OUT_PATH = Path("verify_dim256_result.json")
DIMS: tuple[int, ...] = (1536, 1024, 512, 256, 128)
TOP_K = 5
ALLOWED_FILTERS = (
    "source_table",
    "document_type",
    "game_date",
    "team_id",
    "season_year",
    "player_id",
    "league_type_code",
)


def vector_literal(values: list[float]) -> str:
    """Render a float list as a pgvector literal."""
    return "[" + ",".join(f"{v:.9g}" for v in values) + "]"


def where_clause(filters: dict[str, object]) -> tuple[str, dict[str, object]]:
    """Build the production-equivalent WHERE clause plus bound params."""
    clauses = [
        "embedding IS NOT NULL",
        "(index_status IS NULL OR index_status IN ('ACTIVE','INDEXED'))",
    ]
    params: dict[str, object] = {}
    for idx, (key, value) in enumerate(sorted(filters.items())):
        if key not in ALLOWED_FILTERS:
            msg = f"Unsupported golden filter key: {key}"
            raise ValueError(msg)
        name = f"f{idx}"
        clauses.append(f"{key} = :{name}")
        params[name] = value
    return " AND ".join(clauses), params


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    print(f"golden queries: {len(golden)}", flush=True)

    texts = [str(item["query"]) for item in golden]
    t0 = time.perf_counter()
    service = EmbeddingService(cache_enabled=False)
    vectors = service.get_embeddings_batch(texts)
    embed_secs = time.perf_counter() - t0
    if len(vectors) != len(texts):
        msg = f"Embedding count mismatch: {len(vectors)} != {len(texts)}"
        raise RuntimeError(msg)
    dims_seen = {len(v) for v in vectors}
    if dims_seen != {1536}:
        msg = f"Unexpected query vector dims: {dims_seen}"
        raise RuntimeError(msg)
    print(f"embedded {len(vectors)} queries in {embed_secs:.1f}s (dim=1536, cache off)", flush=True)

    per_query: list[dict[str, object]] = []
    with get_vector_session() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        session.execute(text("SET LOCAL statement_timeout = '300s'"))
        session.execute(text("SET LOCAL enable_indexscan = off"))
        session.execute(text("SET LOCAL enable_bitmapscan = off"))
        session.execute(text("SET LOCAL jit = off"))

        total_rows = session.execute(text("select count(*) from rag_chunks")).scalar()
        print(f"snapshot rows: {total_rows}", flush=True)

        # Preflight: are all golden refs retrievable (not DELETED/missing)?
        refs = sorted({cid for item in golden for cid in item["relevantChunkIds"]})
        status_rows = session.execute(
            text(
                "select source_table || ':' || source_row_id as cid, "
                "coalesce(index_status, '(null)') as st "
                "from rag_chunks "
                "where source_table || ':' || source_row_id = any(cast(:refs as text[]))"
            ),
            {"refs": refs},
        ).fetchall()
        status_map = {r[0]: r[1] for r in status_rows}
        bad_refs = [
            cid for cid in refs if status_map.get(cid) not in ("ACTIVE", "INDEXED")
        ]
        print(
            f"golden refs: {len(refs)} total, {len(refs) - len(bad_refs)} retrievable, "
            f"offending: {bad_refs or 'none'}",
            flush=True,
        )

        for qi, item in enumerate(golden):
            qid = str(item["id"])
            relevant = set(item["relevantChunkIds"])
            filters = dict(item.get("filters") or {})
            where_sql, params = where_clause(filters)

            candidates = session.execute(
                text(f"select count(*) from rag_chunks where {where_sql}"), params
            ).scalar()

            qv = vectors[qi]
            dense_sql = (
                "select source_table || ':' || source_row_id as chunk_id, "
                "(subvector(embedding, 1, cast(:dim as int)) <=> cast(:qv as vector)) as distance "
                f"from rag_chunks where {where_sql} "
                "order by distance asc limit :k"
            )

            dim_rows: dict[str, object] = {}
            for dim in DIMS:
                exec_params = dict(params)
                exec_params["dim"] = dim
                exec_params["qv"] = vector_literal(qv[:dim])
                exec_params["k"] = TOP_K
                t1 = time.perf_counter()
                rows = session.execute(text(dense_sql), exec_params).fetchall()
                elapsed = time.perf_counter() - t1
                top_ids = [r[0] for r in rows]
                hits = sum(1 for cid in top_ids if cid in relevant)
                first_rank = next(
                    (rank for rank, cid in enumerate(top_ids, start=1) if cid in relevant),
                    None,
                )
                dim_rows[str(dim)] = {
                    "top_ids": top_ids,
                    "top_distances": [round(float(r[1]), 6) for r in rows],
                    "hits": hits,
                    "recall": hits / len(relevant),
                    "mrr": (1.0 / first_rank) if first_rank else 0.0,
                    "top1": 1 if first_rank == 1 else 0,
                    "first_rank": first_rank,
                    "seconds": round(elapsed, 3),
                }

            per_query.append(
                {
                    "id": qid,
                    "query": item["query"],
                    "tags": item.get("tags", []),
                    "filters": filters,
                    "relevant": sorted(relevant),
                    "candidates": int(candidates),
                    "dims": dim_rows,
                }
            )
            r1536 = dim_rows["1536"]
            r256 = dim_rows["256"]
            print(
                f"[{qi + 1:02d}/{len(golden)}] {qid:<16} cand={int(candidates):>7} "
                f"1536: hits={r1536['hits']} rank={r1536['first_rank']} "
                f"256: hits={r256['hits']} rank={r256['first_rank']}",
                flush=True,
            )

    # ---- aggregate ----
    summary: dict[str, object] = {}
    for dim in DIMS:
        key = str(dim)
        rows = [q["dims"][key] for q in per_query]
        overlap = []
        for q in per_query:
            a = set(q["dims"]["1536"]["top_ids"])
            b = set(q["dims"][key]["top_ids"])
            overlap.append(len(a & b) / len(a | b) if (a | b) else 1.0)
        summary[key] = {
            "recall_at_5": round(statistics.mean(r["recall"] for r in rows), 4),
            "mrr": round(statistics.mean(r["mrr"] for r in rows), 4),
            "hit_queries": sum(1 for r in rows if r["hits"] > 0),
            "top1_queries": sum(1 for r in rows if r["top1"] == 1),
            "mean_top5_jaccard_vs_1536": round(statistics.mean(overlap), 4),
            "median_seconds": round(statistics.median(r["seconds"] for r in rows), 3),
        }

    base = "1536"
    regressions: list[dict[str, object]] = []
    improvements: list[dict[str, object]] = []
    for q in per_query:
        b = q["dims"][base]
        d = q["dims"]["256"]
        if d["hits"] < b["hits"] or (b["top1"] == 1 and d["top1"] == 0):
            regressions.append(
                {
                    "id": q["id"],
                    "1536_hits": b["hits"],
                    "256_hits": d["hits"],
                    "1536_rank": b["first_rank"],
                    "256_rank": d["first_rank"],
                    "1536_top5": b["top_ids"],
                    "256_top5": d["top_ids"],
                }
            )
        if d["hits"] > b["hits"] or (d["top1"] == 1 and b["top1"] == 0):
            improvements.append({"id": q["id"], "1536_hits": b["hits"], "256_hits": d["hits"]})

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "golden_path": str(GOLDEN_PATH),
        "golden_queries": len(golden),
        "snapshot_rows": int(total_rows),
        "embedding_seconds": round(embed_secs, 1),
        "dims": list(DIMS),
        "top_k": TOP_K,
        "summary": summary,
        "regressions_1536_to_256": regressions,
        "improvements_1536_to_256": improvements,
        "per_query": per_query,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n== summary (dense leg only, exact scans, prod filters) ==", flush=True)
    header = f"{'dim':>6} {'recall@5':>9} {'MRR':>7} {'hit@5':>6} {'top1':>5} {'jac_vs1536':>11} {'median_s':>9}"
    print(header, flush=True)
    for dim in DIMS:
        s = summary[str(dim)]
        print(
            f"{dim:>6} {s['recall_at_5']:>9.4f} {s['mrr']:>7.4f} {s['hit_queries']:>6} "
            f"{s['top1_queries']:>5} {s['mean_top5_jaccard_vs_1536']:>11.4f} {s['median_seconds']:>9.3f}",
            flush=True,
        )
    print(f"\n1536 -> 256 regressions: {len(regressions)}", flush=True)
    for r in regressions:
        print(
            f"  {r['id']}: hits {r['1536_hits']}->{r['256_hits']}, "
            f"rank {r['1536_rank']}->{r['256_rank']}",
            flush=True,
        )
    print(f"1536 -> 256 improvements: {len(improvements)}", flush=True)
    for r in improvements:
        print(f"  {r['id']}: hits {r['1536_hits']}->{r['256_hits']}", flush=True)
    print(f"\nwrote {OUT_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
