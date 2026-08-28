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

## Current Oracle Cutover Status (2026-08-22)

The approved Oracle production cutover is complete. The external
`207,270`-chunk source snapshot was loaded into the canonical Oracle
`rag_chunks` table, with `207,270` active rows, `207,270` native
`EMBEDDING_VECTOR` values, and `207,270` distinct source identities. The
rollback table `RAG_CHUNKS_BAK_20260821` preserves the pre-cutover contents.

The native `VECTOR(1536,FLOAT32,DENSE)` column and
`IDX_RAG_CHUNKS_EMBEDDING_HNSW` index are valid and visible. The final
`audit_rag_index --require-nonempty --json` report is consistent with
`healthy=207270` and zero sparse-only, vector-only, orphan, hash/version,
stale, missing-embedding, or deleted rows. `apply_oracle_migrations --check`
also reports the Oracle migration chain in sync.

The PostgreSQL `kbo_pgvector_promotion_target:55434` database remains an
isolated canary/staging artifact and is not the production backend. Oracle is
selected through `DATABASE_URL`; `RAG_INDEX_DB_URL`, `PGVECTOR_URL`, and
`PGVECTOR_TEST_URL` must remain unset for the production evaluation path.

The configured 30-query Oracle replay passes the quality gates: vector Recall@5
`0.9152` / MRR `0.8028`, resolver-hybrid Recall@5 `0.9485` / MRR `0.8361`, and
hit rate `0.9667`. Routing remains `100/100` with zero entity false positives.
Oracle hybrid latency is still above the historical `500ms` p95 target and is
variable across remote runs (approximately `1.4s` to `5.5s` p95). The
remaining performance gap is an Oracle sparse/CLOB search limitation; do not
declare the latency gate closed until an Oracle-native sparse index or an
explicit threshold decision is made.

### Live Reconciliation (2026-08-22)

The approved snapshot remains `207,270` rows. A post-cutover read-only census
found `165` pre-existing active rows outside that snapshot (`futures_schedule`
126 and `press_release` 39). They were not deleted; they were embedded with
the approved `perplexity/pplx-embed-v1-4b` 1536-dimensional configuration.
The current live Oracle audit therefore reports `207,435` identities and
`207,435` valid vectors with `consistent=true`. Keep the rollback table until
the operator confirms whether these 165 rows are part of the permanent
production corpus.

### Oracle Sparse Term Index (2026-08-22)

Oracle Text is unavailable in the production service, so the opt-in sparse
term path uses the derived `RAG_CHUNK_TERMS` postings table. Migrations
`068_create_rag_chunk_terms.sql` and `069_add_rag_chunk_term_source_scope.sql`
are applied. After the scheduler's 2026-08-22 incremental publish and a
catch-up resume build, the live table contains `4,095,932` postings covering
all `209,537` active/indexed chunks, with zero NULL source scopes, orphan
rows, or active chunks missing postings. `IDX_RAG_CHUNK_TERMS_TOKEN_CHUNK`,
`IDX_RAG_CHUNK_TERMS_GAME_DATE`, `IDX_RAG_CHUNK_TERMS_SOURCE_TOKEN`, and
`IDX_RAG_CHUNK_TERMS_SOURCE_DATE` are `VALID`, and table/index statistics were
gathered after the rebuild.

As of 2026-08-23 `RAG_ORACLE_SPARSE_MODE` defaults to `terms`; set it to
`legacy` to roll back to the CLOB candidate path. Both retrieval legs are
index-bounded:

Sparse: one STOPKEY postings slice per token (`TOKEN` prefix of
`IDX_RAG_CHUNK_TERMS_TOKEN_CHUNK`, or `IDX_RAG_CHUNK_TERMS_TOKEN_SOURCE` when a
source filter is present), then weighted counts by primary key and a Python
merge. Chunk-column filters (team/season/player/index_version) keep the joined
scored path. Full rows are fetched only for the top-scored buffer (top_k x 8,
floor 40). Query keywords are punctuation-stripped like document tokens.

Vector: `defer(embedding)` keeps the 1536-dim column out of every fetch;
scalar-filtered searches pre-resolve candidate IDs via new B-tree indexes
(`IDX_RAG_CHUNKS_TEAM_ID`, `IDX_RAG_CHUNKS_SEASON_YEAR`,
`IDX_RAG_CHUNKS_PLAYER_ID`, `IDX_RAG_CHUNKS_TEAM_SEASON`) and run exact
distance over the ID set when it is small (<=200); larger sets use the global
approximate fetch plus Python post-filter.

Measured pre-parallel steady-state on a quiet instance (2026-08-23 canary,
configured embeddings): BM25 Recall@5 `0.6000` / MRR `0.4667` at p50
`183-318ms`, p95 `433-659ms`; resolver-hybrid Recall@5 `0.9485` / MRR
`0.8306`, hit rate `0.9667`, warm p50 `509-636ms`. The sparse-only path
meets the historical `500ms` p95 target. Hybrid was sequential-leg bound
until the leg-parallel change documented below. Cold-cache first touches and
concurrent writer contention can still inflate single-run p95 into seconds.
Term maintenance runs whenever an Oracle session writes chunks while the mode
resolves to `terms` (now the default); `RAG_ORACLE_SPARSE_MODE=legacy` disables
both the postings search and incremental maintenance.

### Parallel Hybrid Canary (2026-08-26)

`HybridRetriever` now overlaps the dense leg with the foreground BM25 leg;
fusion still runs after both legs complete. Three consecutive runs against the
30-query configured-embedding golden set held quality at Recall@5 `0.9485`,
MRR `0.8306`, and hit rate `0.9667`:

```text
run   p50       p95       max
1     233.046ms 466.859ms 693.753ms
2     162.055ms 376.774ms 569.397ms
3     156.738ms 362.485ms 549.355ms
```

These quiet-instance runs meet the `500ms` hybrid p95 target. Repeat the
canary after scheduler cutover and under normal writer load before treating
the result as a permanent production SLO.

### Historical Production Reindex (2026-08-27)

The validated 1982-2000 historical corpus was embedded into the canonical
Oracle `rag_chunks` table using the configured OpenRouter provider. The scoped
build covered `game`, `team_standings_daily`, `player_season_batting`, and
`player_season_pitching`: `10,890` new embeddings were published, while
`3,409` matching identities were reused. Provider rate-limit responses were
handled by the configured retry policy.

The post-build Oracle audit reports `220,429` total rows, `218,408` healthy
retrievable rows, `2,021` explicitly `DELETED` stale identities, zero
sparse-only/vector-only/orphan/hash/version/embedding findings, and zero
active chunks missing sparse postings. The historical active identity census
matches the primary source exactly: `game=8,237`,
`team_standings_daily=584`, `player_season_batting=2,960`, and
`player_season_pitching=2,518`.

The subsequent metadata-only repair normalized `153,970` legacy
`season_year` values across `game`, `game_lineups`, `game_play_by_play`, and
`game_highlights`; `20,271` rows were already correct. It changed no content,
embedding, hash, or lifecycle fields, and the immediate idempotency dry-run
reported zero remaining candidates. The durable validation record is
`Docs/references/rag_production_validation_20260828.json`.

A 30-query configured replay against this production corpus reported:

```text
variant          recall@5  MRR     hit rate  p50       p95
bm25             0.5667    0.4500  0.5667    618ms     26,282ms
vector           0.9152    0.7972  0.9333    248ms     1,789ms
hybrid           0.9485    0.8306  0.9667    325ms     1,746ms
resolver_hybrid  0.9485    0.8306  0.9667    196ms     670ms
```

The post-repair replay retained the same quality metrics. Its p95 values were
`11,256ms` for BM25, `2,180ms` for vector, `3,438ms` for hybrid, and `743ms`
for resolver-hybrid. These remote-run measurements keep the `500ms` latency
SLO open despite the earlier quiet-instance canary passing; repeat under a
controlled cache and writer-load condition before making an SLO decision.

An additional resolver-only replay showed the source of the variability: two
runs were below `500ms` p95 (`381ms` and `498ms`), while one reached `20,256ms`.
A deterministic embedding control still varied between `420ms` and `603ms`
p95, so the tail is not attributable to the provider alone. Query traces point
to both Oracle sparse/date-filter work and occasional resolver/vector calls;
the detailed measurements are preserved in
`Docs/references/rag_production_validation_20260828.json`.

This replay is evidence after the historical reindex, not a replacement for
the quiet-instance canary or a permanent SLO decision. The query path now
extracts explicit four-digit years and aligns punctuation tokenization with
the Oracle postings builder; an exact `1988-04-02 OB 4 LT 0` smoke query
returns `game:19880402OBLT0` first.

The latency path was then optimized in two small ways. A query year is no
longer added when an explicit `game_date` already determines it, and
`source_table + game_date` are pushed into the bounded Oracle postings slice
using the corresponding composite index. Representative sparse calls changed
from cold/warm samples of up to `38.7s` for a redundant season join to
`680/146/120ms` for game, `386/119/131ms` for PBP, and `199/121/130ms` for
highlight date filters. Retrieval quality was unchanged.

Three subsequent configured resolver-hybrid golden-set runs reported:

```text
run  recall@5  MRR     hit rate  p50       p95       max
1    0.9485    0.8306  0.9667    188ms     267ms     373ms
2    0.9485    0.8306  0.9667    228ms     287ms     411ms
3    0.9485    0.8306  0.9667    223ms     277ms     390ms
```

This is a passing optimized-path canary, but the permanent `500ms` SLO
remains pending confirmation during a scheduled writer-load cycle.

Postings freshness is operationally guarded:

```bash
# Daily 05:40 KST job — insert-only resume after the last indexed chunk
python3 -m src.cli.rag.build_oracle_sparse_index --apply --catch-up --batch-size 40

# Coverage gate — fails when retrievable chunks lack postings
python3 -m src.cli.audit_rag_index --require-nonempty --require-postings --json
```

`sparse_terms_catchup_job` runs after the 05:00 incremental publish so chunks
added without the terms flag are indexed on the same schedule;
`--catch-up` resolves the resume point from `max(rag_chunk_id)` and never
rewrites existing postings (the Oracle delete-then-reinsert path deadlocks).
The audit reports `postings_missing` on every run and `--require-postings`
turns any gap into an exit-code failure for CI/alerting.
APScheduler loads jobs at process startup, so a running scheduler must be
restarted once (`launchctl kickstart -k gui/$UID/com.kbo-playwright.scheduler`)
before the 05:40 registration takes effect; until then run the catch-up
command above manually after each publish.

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

### AI Runtime Oracle Cutover Readiness (2026-08-23)

The KBO_platform `bega_AI` runtime now ships a native-Oracle RAG adapter
(`app/core/oracle_rag.py`) validated against the live ADB wallet connection:

- Live smoke (10/10 green): pool open, readiness (`209,537` rows / vectors,
  dim 1536, HNSW valid), dense cosine search, dense+sparse RRF fusion, exact
  CLOB document search with dict metadata, source-filtered search, MERGE
  upsert with term postings (verified in-transaction), rollback residual 0.
- Adapter fixes surfaced by the live run: sync-return `create_pool_async`,
  `wallet_location` + PEM passphrase fallback (`OCI_WALLET_PASSWORD` or the
  URL password), async LOB reads for `title`/`content`/`meta`, and CLOB-safe
  ordering.
- The PostgreSQL-only batch writer is refused at startup when
  `AI_RAG_DB_URL` is an Oracle URL; manual `/ai/ingest` writes both
  `rag_chunks` and `rag_chunk_terms`.
- Operator decisions (2026-08-23): the post-snapshot growth is retained as
  production corpus (live total `209,537` and continuing via the concurrent
  writer); the rollback table `RAG_CHUNKS_BAK_20260821` and the PostgreSQL
  IVFFlat index are kept until a later maintenance cycle.

Server cutover remains: activate the three `.env.prod` values (already staged
locally), rebuild/restart the `ai-chatbot` container, then verify
`/ready` `checks.db_rag` and `checks.vector_index.ready=true`.

### Oracle Adapter Latency Gate (2026-08-23)

Live probes from a remote client (home network, ADB `medium`) isolated and
fixed three compounding costs in the new AI-runtime adapter:

| Fix | Effect |
| --- | --- |
| `oracledb.defaults.fetch_lobs = False` | CLOB/JSON columns return inline; 24-row dense hit dropped from ~16.5s to ~110ms of transfer |
| `FETCH APPROX FIRST` dense ordering | HNSW-eligible plan documented; scan itself was never the bottleneck (~200ms raw) |
| Per-token postings top-N + Python merge, identity-only candidates, hydrate fused survivors only | hybrid RRF fell from >60s timeout → 4.5s → **0.77–0.96s** |

Measured after fixes (limit=24, remote client):

```text
dense_only   p50 ~300ms   best 215ms
hybrid_rrf   ~0.8-1.0s    (was: timeout at 60s)
exact_doc    0.14-1.5s    (legacy CLOB substring path, tools-level)
```

Dense retrieval meets the historical `500ms` target even from the remote
client. Hybrid RRF is now RTT-bound on hydration of sparse-only survivors
(~25ms/row remote); it must be re-measured from the production server next to
the ADB before declaring the latency gate closed. The exact-document tool
path remains the documented Oracle CLOB-scan limitation.

### Golden Query Evaluation After Corpus Growth (2026-08-23)

Re-ran `evaluate_rag_retrieval --embedding-mode configured --top-k 5`
against live ADB (30 golden queries, dataset sha256 `6753148…`):

```text
variant          recall@5   mrr      hit_rate   p50        p95
hybrid           0.9485     0.8306   0.9667     1.71s      7.57s
resolver_hybrid  0.9485     0.8472   0.9667     1.12s      2.18s
```

Recall/hit-rate hold at the approved levels after the corpus grew from
`207,270` to `209,537` rows; resolver-hybrid MRR improved (`0.8361 →
0.8472`). Remote-client latency remains RTT-dominated and must be re-read
from the production server after cutover.

### Production Promotion Gates (approved 2026-08-23)

Operator-approved minimums for the live Oracle RAG index:

```text
Recall@5      >= 0.90   (golden 30-query set, configured embeddings)
MRR           >= 0.80
Hit rate      >= 0.95
Routing acc   >= 0.98
```

Current values pass all four gates (0.9485 / 0.8472 / 0.9667 / 100%).
Latency stays excluded from the hard gate until the server-side hybrid p95
re-measurement lands post-cutover.

### Current Corpus Canary (2026-08-28)

The live index audit now reports `221,554` total/vector rows, `219,533`
healthy active rows, `2,021` tombstoned rows, and zero orphan, hash, version,
embedding, or sparse-posting gaps. A fresh 30-query configured-embedding
`resolver_hybrid` run remains above the approved quality gates:

```text
Recall@5  0.9485   MRR 0.8306   hit rate 0.9667
p50 408ms         p95 17.2s     max 29.2s
```

The quality result is accepted. The remote-client p95 is highly variable and
is not promoted to a hard latency gate until measured from the production
server or an ADB-adjacent runner.
