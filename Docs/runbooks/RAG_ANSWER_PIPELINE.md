# RAG Answer Pipeline

How a question becomes an answer, and what to check when it does not.

## 1. The path

```
question
  → QueryRouter          (structured or hybrid route)
  → StructuredRetriever  (정형 통계 질문: 선수·시즌·지표)
     or HybridRetriever  (BM25 ∥ vector, fused — AGENTS.md Phase 86)
  → RagAnswerService     (chat model, evidence-only prompt)
  → answer + sources
```

`HybridRetriever` exposes `last_trace`, whose `latency_ms` breaks the search
into `bm25` / `embedding` / `vector` / `fusion` / `reranker`. Read it before
suspecting the wrong layer.

## 2. Three failures that all looked like "the data is missing"

Measured 2026-10-28. Each one produced an *empty or erroring* search while the
rows were present. They are listed together because each was hidden by the one
before it, and none of them is visible to a unit test that mocks the search.

| Symptom | Cause | Fix |
| --- | --- | --- |
| `column rag_chunks.document_type does not exist` | `RagChunkVector` expects 5 columns (`document_type`, `game_date`, `published_at`, `source_url`, `language`) that `migrations/pgvector/003` adds — but that chain has **no runner**. | `migrations/postgresql/066_rag_chunks_metadata_columns.sql` |
| `operator does not exist: date = character varying` | The `game_date` filter is typed `str` and bound straight into a `DATE` comparison. | `_coerce_game_date` in `vector_search_repository.py` |
| Search took 232s (a bare `count(*)`), 15min+ end to end | **No vector index.** 377k rows × 1536 dims scanned in full. `migrations/pgvector/006` creates it, again with no runner. | `migrations/postgresql/068_rag_chunks_embedding_index.sql`; applied with `CONCURRENTLY` (46 min, 2.9 GB) |

**The pattern to remember**: the `migrations/pgvector/` chain was written for a
local Docker instance and never wired to a runner. Anything it adds must be
checked against the live database, because the model will happily query columns
that do not exist until someone runs a real search.

**Verification that would have caught all three**: run an actual query. The
existing tests mock `search_by_cosine`, so they pass with the schema broken.

## 3. Asking a question

```bash
# CLI (search only — shows chunks and scores)
python3 -m src.cli.rag.query --query "2004년 MVP는 누구인가?" --top-k 5

# API (answer + sources)
curl -X POST "$KBO_API/api/v1/rag/ask" \
  -H "X-API-Key: $KBO_API_KEY" -H "Content-Type: application/json" \
  -d '{"query": "2004년 MVP는 누구인가?", "top_k": 5}'
```

`answer` is the model's prose. `sources` lists the evidence with the query
terms bolded in `snippet`. `chunk_count` is how many chunks the search returned.

## 4. What the answer layer will not do

`RagAnswerService` sends `ANSWER_RULES` with every call. The rules are in the
source rather than left to a caller, because this corpus has known holes:

* it answers **only from the retrieved evidence** and is told not to fill gaps
  from its own knowledge;
* when the evidence is silent it returns **"제공된 기록에서 확인할 수 없습니다"**.

So a question about a game whose box score was never collected (2001~2007 has
126~248 of 3,642 games) yields that sentence, not a plausible score. That is the
intended behaviour — see `Docs/runbooks/HISTORICAL_IMPORT.md` for what is
missing and how to import it when an outside source provides it.

The rules are versioned by their wording: `ANSWER_RULES` is asserted verbatim in
`tests/services/test_rag_answer_service.py`, so changing the policy means
changing that test.

## 5. When the model is unavailable

The service never returns an empty answer:

| State | `llm_used` | `fallback_reason` | `answer` |
| --- | --- | --- | --- |
| answered | true | none | model text |
| no chunks retrieved | false | `no_evidence` | "제공된 기록에서 확인할 수 없습니다" |
| no `OPENROUTER_API_KEY` | false | `no_api_key` | the chunks, listed |
| transport error | false | `transport_error:<Type>` | the chunks, listed |
| non-200 | false | `http_<status>` | the chunks, listed |
| unusable envelope | false | `malformed_response` | the chunks, listed |
| empty completion | false | `empty_completion` | the chunks, listed |

The distinction matters operationally: `no_evidence` means the corpus has
nothing, the others mean the summariser failed while the evidence was fine.

Model and key: `ANSWER_MODEL` (default `openai/gpt-4o-mini`) and
`OPENROUTER_API_KEY`. The chat model is deliberately separate from the embedding
model — they are different contracts.

## 6. Performance notes

* **Vector**: with `idx_rag_chunks_embedding_ivfflat` (lists=100),
  `ivfflat.probes = 1` measured **78 ms**; probes=10 measured **18 s**. The
  index is only useful at low probe counts, and probe count trades recall for
  time — measure recall with `python3 -m src.cli.rag.evaluate_rag_retrieval`
  before raising it.
* **BM25** was the larger half of a typical search (3.2 s of 3.2 s total in one
  trace) — worth looking at first when a search feels slow.
* **Occasionally far slower** (145 s, 395 s observed). `_load_filtered_search_results`
  runs `SET LOCAL enable_indexscan = off` and rescans exactly when a filtered
  search returns nothing, which disables the vector index for that statement.
  `SET LOCAL` clears at transaction end, so the session is not poisoned
  afterwards — but the statement itself pays the full scan. Narrow filters that
  match nothing are the expensive case.

## 7. Building the vector index

`068` carries the statement. On a large existing table, apply it out of band so
writes are not locked:

```sql
SET maintenance_work_mem = '128MB';   -- lists=100 needs 65MB; the default is 64MB
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rag_chunks_embedding_ivfflat
    ON rag_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
```

Two things learned the hard way:

* **`CONCURRENTLY` failures leave an INVALID index.** Check `pg_index.indisvalid`
  and `DROP INDEX CONCURRENTLY` before retrying, or the retry silently keeps the
  broken one.
* **A 512 MB `maintenance_work_mem` was followed by the server dropping the
  connection** mid-build (`server closed the connection unexpectedly`, uptime
  resetting to 1:47). 128 MB completed the build. Treat a large `work_mem`
  bump on this host as a risk, not a free speedup.
