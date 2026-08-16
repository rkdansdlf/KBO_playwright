# RAG Retrieval Evaluation

The retrieval evaluation command uses a labeled JSON array and does not call an
LLM. Each case must identify the stable chunk identity used by the indexes:
`source_table:source_row_id`.

Example:

```json
[
  {
    "query": "2024년 김도영 홈런 수",
    "relevantChunkIds": ["player_season_batting:123"],
    "intent": "STAT_QUERY",
    "filters": {"season_year": 2024, "player_id": "78224"}
  }
]
```

Run an evaluation with:

```bash
python3 -m src.cli.evaluate_rag_retrieval \
  --dataset Docs/references/rag_golden_queries.json \
  --top-k 5 --json
```

The report includes Recall@K, Precision@K, MRR, and hit rate. Keep the dataset
versioned and compare the same cases when changing entity resolution, filters,
fusion weights, rerankers, or embedding models. It also records p50/p95/max
retrieval latency and dataset/index/embedding metadata. Use `--output` to save
the report and `--min-recall`/`--min-mrr` to return exit code 1 when a quality
threshold is missed.

The repository contains a deterministic, source-to-chunk fixture corpus for
acceptance wiring. It is intentionally separate from the production golden
set:

```bash
python3 -m src.cli.bootstrap_rag_eval_corpus --json
```

The command validates `tests/fixtures/rag_corpus/documents.json` through
`TextTransformer` and checks all 100 references in
`tests/fixtures/rag_corpus/golden_queries.json`. It never writes by default.
For an isolated PostgreSQL plus pgvector test pair, set `RAG_TEST_DB_URL`,
`PGVECTOR_TEST_URL`, and `RAG_EVAL_ALLOW_WRITE=1`, then run:

```bash
python3 -m src.cli.bootstrap_rag_eval_corpus --apply --json
python3 -m src.cli.audit_rag_index --require-nonempty --json
python3 -m src.cli.evaluate_rag_retrieval \
  --dataset tests/fixtures/rag_corpus/golden_queries.json \
  --corpus tests/fixtures/rag_corpus/documents.json \
  --all-variants --embedding-mode deterministic \
  --output rag-retrieval-evaluation.json --json
```

The fixture index uses the same sparse/vector upsert path as
`build_rag_index`; rows remain `PENDING` until both stores have been written,
then become `ACTIVE`. Do not use the fixture metrics as production retrieval
quality evidence. Production evaluation still requires annotated IDs from the
real indexed corpus.

Routing is evaluated separately with a dataset containing `expectedIntent`,
`expectedRoute`, and optional `expectedEntities` fields:

```bash
python3 -m src.cli.evaluate_rag_routing \
  --dataset Docs/references/rag_routing_golden_queries.json --json
```

The routing report includes intent accuracy, route accuracy, entity accuracy,
and entity-negative false-positive rate. Retrieval evaluation rejects an empty
dataset or a case without `relevantChunkIds` with exit code 2.

The index audit contract is:

```text
0 = both indexes reachable and no identity mismatch
1 = both indexes reachable and at least one mismatch
2 = infrastructure or execution failure
```

Its JSON report includes `total`, `healthy`, `sparse_only`, `vector_only`,
`orphan`, `hash_mismatch`, `version_mismatch`, `stale`, and `deleted` counts.
The identity backfill command is dry-run by default:

```bash
python3 -m src.cli.backfill_rag_index_identity --json
python3 -m src.cli.backfill_rag_index_identity --apply --json
```

Explicit source mutations use the same pending-to-active lifecycle. Delete is
also dry-run unless `--apply` is supplied:

```bash
python3 -m src.cli.propagate_rag_index \
  --source-table team_events --source-row-id 123 --delete --json
python3 -m src.cli.propagate_rag_index \
  --source-table team_events --source-row-id 123 \
  --payload /path/to/chunk-update.json --apply --embedding-mode configured --json
```
