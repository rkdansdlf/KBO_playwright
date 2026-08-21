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

For a read-only production-source inventory, use the same source iterators as
the index builder without embedding or database writes:

```bash
python3 -m src.cli.inventory_rag_corpus --source all --limit 1000 --json
python3 -m src.cli.inventory_rag_corpus --source batting --season 2025 --json
python3 -m src.cli.inventory_rag_corpus --source all \
  --require-source players --require-source awards \
  --output reports/rag-corpus-inventory.json --json
python3 -m src.cli.inventory_rag_corpus --source all \
  --profile production --output reports/rag-corpus-inventory.json --json
```

The inventory reports generated chunks, new/updated/unchanged candidates,
duplicate or invalid identities, missing metadata, and estimated embedding
requests. `--limit` makes the report non-complete and therefore suppresses the
delete census. A complete-scope report returning duplicate identities is a
blocking corpus defect, not a retrieval-quality failure.
Use `--require-source` for domains that must be present in a production
corpus; an empty optional source is reported but does not fail the command.
The tracked source contract is `Docs/references/rag_source_contract.json`.
Production and staging require the core player, statistics, game, lineup, PBP,
standings, ranking, team, and movement sources. Awards, events, highlights, and
documentation sources are enrichment and remain optional.
The complete-scope manifest includes per-source `source_rows`, `chunks_generated`,
`new`, `unchanged`, `updated`, `deleted`, `elapsed_ms`, and defect counts, plus
aggregate totals. Do not use a run with `--limit` as the production baseline.
Before staging or production indexing, require the Oracle `DATABASE_URL` and an
embedding provider key; configured embedding runs use `OPENROUTER_API_KEY`.
Oracle uses one `rag_chunks` table for sparse and dense state, so
`RAG_INDEX_DB_URL` is normally unset. `audit_rag_index --require-nonempty` must
be run after Oracle migrations and after the batch publish. Non-dry-run staging
builds require `RAG_TARGET_ENV=staging` and `RAG_INDEX_ALLOW_WRITE=1`; production
builds additionally require `RAG_TARGET_ENV=production` and
`RAG_INDEX_ALLOW_PRODUCTION_WRITE=1`. The target URL is redacted in logs.
`RAG_EMBED_BATCH_SIZE` defaults to 50 to keep long source documents below
provider batch/rate limits; increase it only after a provider-specific canary.
The manifest exposes `deleted_identities` so stale chunks can be reviewed
before applying deletes.
For staging lifecycle/scale acceptance without an external embedding provider,
`build_rag_index --embedding-mode deterministic` is available. Do not use its
metrics as production retrieval-quality evidence; use `--embedding-mode
configured` with the selected provider for the production baseline.

The latest isolated recovery-staging run indexed the current source iterator
census of `207,305` chunks into both sparse and configured-vector stores. The
final audit reported `207,305` healthy rows, zero sparse-only/vector-only rows,
zero hash/version mismatches, zero missing embeddings, and `consistent=true`.
The configured vector store uses `perplexity/pplx-embed-v1-4b`, 1536 dimensions,
valid full/PBP HNSW indexes, and date/text filter indexes. This is a staging
checkpoint, not a permanent corpus-size contract; regenerate the complete
inventory when the source database changes.

The configured-provider golden set contains 30 queries and uses a multi-label
correction for the 2026-06-26 HH-SSG BIG_PLAY question. The final replay reports
BM25 Recall@5 `0.6000` / MRR `0.5444`, vector `0.9818` / `0.8722`, hybrid
`0.9818` / `0.8889`, and resolver-hybrid `0.9818` / `0.8889`; all dense and
hybrid variants have hit rate `1.0`. Resolver-hybrid p95 latency was `386.91ms`
in the recorded replay. Routing remains `100/100` for intent, route, and
entity accuracy with zero false positives. Golden-label confirmation, quality
thresholds, and provider budget remain pending human approval. See
`Docs/references/rag_configured_cost_evidence.json` for measured and explicitly
unmeasured cost fields. Production promotion remains blocked.

## Current Oracle Cutover Status (2026-08-21)

The current Oracle RAG corpus has completed its configured embedding batch.
The native `rag_chunks.embedding_vector VECTOR(1536)` column and HNSW index
are valid, and the final audit reports `21,589` sparse/vector identities,
`21,583` active rows with embeddings, six deleted tombstones without vectors,
zero sparse-only/vector-only rows, zero hash/version mismatches, zero stale
rows, `embedding_missing=0`, and `consistent=true`. Direct Oracle cosine
retrieval returned results from both documentation and player-stat sources.

This is complete for the current Oracle corpus, not a claim that the separate
`207,305`-chunk source-iterator census has been copied into Oracle. The
production source decision is now direct Oracle (`DATABASE_URL`); do not set
`RAG_SOURCE_DB_URL` unless a separate source database is intentionally used.

The PostgreSQL recovery vector database is an isolated staging artifact only.
It contains the `207,305`-row configured replay corpus and must not be treated
as the Oracle production corpus. The production runtime configuration still
contains the legacy `EMBED_DIM=256` and `text-embedding-3-small` settings, and
no AI runtime container is currently serving traffic.

The embedding provider/model, 1536 dimensions, direct Oracle source, and
Oracle write window were approved and used for the batch. The final runtime
restart/cutover remains pending because the AI backend still targets
PostgreSQL/pgvector. Keep IVFFlat indexes in place until the selected runtime
has passed the Oracle retrieval smoke test; index cleanup is a separate
approval.

Audit the awards source path before attempting a production save:

```bash
python3 -m src.cli.audit_awards --json --output reports/awards-audit.json
python3 -m src.cli.audit_awards --probe --json --output reports/awards-audit-probe.json
```

`--probe` fetches and parses Wikipedia/Yagoonara without saving. The report
separates raw snapshots, parsed records, stored rows, source errors, pending
parser status, schema gaps, natural-key duplicates, invalid season values, and
invalid player/team links. A `persistence_missing` result means the live
sources parsed records but the target `awards` table still has no rows.

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
