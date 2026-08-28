# RAG Writer-Cycle Canary (2026-08-28)

## Scheduler Evidence

The post-cutover scheduler cycle completed on the canonical Oracle database:

| Job | Scheduled | Observed result |
| --- | --- | --- |
| `sync_rag_incremental` | 05:00 KST | Started at 05:00:00, completed successfully at 05:16:19 |
| `sparse_terms_catchup` | 05:40 KST | Completed successfully at 05:40:00.911 |
| `rag_audit_sentinel` | 06:05 KST | Started late at 06:13:07 and completed successfully at 06:13:13 |

The sentinel was delayed by the preceding scheduler workload, but it completed
successfully. No new `ORA-12860` or `PendingRollbackError` was observed in the
05:00-06:50 KST writer cycle.

## Oracle Audit

The read-only audit after the cycle reported:

```text
primary_count=221554  vector_count=221554  healthy=219533  deleted=2021
postings_missing=0    orphan=0              hash_mismatch=0
version_mismatch=0    embedding_missing=0   stale=0
consistent=true
```

## Retrieval Canary

Three consecutive `resolver_hybrid` runs used the 30-query configured-
embedding golden set after the writer cycle:

```text
run   recall@5  mrr     hit_rate  p50       p95       max
1     0.9485     0.8306  0.9667    196.692ms 447.406ms 618.210ms
2     0.9485     0.8306  0.9667    214.990ms 399.037ms 1101.366ms
3     0.9485     0.8306  0.9667    201.428ms 446.109ms 675.516ms
```

All three runs passed the quality gates and the hybrid p95 target of 500ms.
This is post-writer evidence, not simultaneous writer-load evidence; retain
the next scheduled cycle as the production SLO confirmation. A prior
post-cutover cold-start run reached 18.4s p95, so cold-start latency remains a
separate operational observation.
